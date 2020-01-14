/*
 * Copyright (C) 2014 Yutaro ISHIDA.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


//--------------------------------------------------
//オントロジのROSラッパー
//
//author: Yutaro ISHIDA
//
//memo:
//トリプル
//主語（subject，～は），述語（predicate，～する），目的語（object，～を）
//
//SELECT ?p  WHERE {<http://www.semanticweb.org/nedo_prius/ontologies/2016/9/untitled-ontology-71#MyCar>  ?p  <http://www.toyota-ti.ac.jp/Lab/Denshi/COIN/Control#TurnRight>}
//
//TODO:
//ログ表示のON，OFFを切り替えできるようにする
//--------------------------------------------------


package com.github.rosjava.ontoros_pkg_i1.ontology_prj_i1;


import org.ros.node.AbstractNodeMain;
import org.ros.node.ConnectedNode;
import org.ros.node.Node; 

//ROSノード名の定義
import org.ros.namespace.GraphName;

//ループ
import org.ros.concurrent.CancellableLoop;

//Publish，Subscribe関連
import org.ros.node.topic.Publisher;
import org.ros.node.topic.Subscriber;
import org.ros.message.MessageListener;

//Parameter関連
import org.ros.node.parameter.ParameterTree;


import java.io.*;
import java.util.*;
import java.text.SimpleDateFormat;

import com.clarkparsia.pellet.owlapiv3.PelletReasoner;
import com.clarkparsia.pellet.owlapiv3.PelletReasonerFactory;

import org.mindswap.pellet.KnowledgeBase;
import org.mindswap.pellet.jena.PelletInfGraph;

import com.clarkparsia.pellet.sparqldl.jena.SparqlDLExecutionFactory;

import com.hp.hpl.jena.rdf.model.InfModel;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Resource;

import org.semanticweb.owlapi.apibinding.OWLManager;

import org.semanticweb.owlapi.model.IRI;

import org.semanticweb.owlapi.model.OWLAxiom;

import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import org.semanticweb.owlapi.model.OWLDataFactory;

import org.semanticweb.owlapi.model.OWLNamedIndividual;

import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLClassAssertionAxiom;

import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDataPropertyAssertionAxiom;

import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;

import org.semanticweb.owlapi.util.InferredAxiomGenerator;
import org.semanticweb.owlapi.util.InferredClassAssertionAxiomGenerator;
import org.semanticweb.owlapi.util.InferredOntologyGenerator;
import org.semanticweb.owlapi.util.InferredPropertyAssertionGenerator;

import org.semanticweb.owlapi.util.SimpleIRIMapper;
 

public class OntologyWrapper extends AbstractNodeMain{
    //--------------------------------------------------
    //メンバ変数（クラス内グローバル変数）
    //--------------------------------------------------
    static private OWLOntologyManager manager;
    static private OWLOntology ontology;

    static private OWLDataFactory factory = null;

    static private PelletReasoner reasoner;

    static private KnowledgeBase kb; 
    static private InfModel model;
    static private PelletInfGraph graph;
    static private Query query;
    static private QueryExecution query_exe; 



    //--------------------------------------------------
    //ROSノード名を決定する関数（ROSノード起動直後くらいに走ってるはず）
    //--------------------------------------------------
    @Override
    public GraphName getDefaultNodeName(){
        return GraphName.of("rosjava/OntologyWrapper");
    }


    //--------------------------------------------------
    //ROSノード起動時に走る関数
    //--------------------------------------------------
    @Override
    public void onStart(final ConnectedNode connectedNode){
        //owlファイルを保存するトピックのSubscriber
        Subscriber<std_msgs.String> sub_save_owl_file = connectedNode.newSubscriber("/ontoros/save_owl_file", std_msgs.String._TYPE);
        sub_save_owl_file.addMessageListener(new MessageListener<std_msgs.String>(){
            @Override
            public void onNewMessage(std_msgs.String message){
                save_owl_file(message.getData());
            }
        });

        //推論するトピックのSubscriber
        Subscriber<std_msgs.Bool> sub_run_reasoner = connectedNode.newSubscriber("/ontoros/run_reasoner", std_msgs.Bool._TYPE);
        sub_run_reasoner.addMessageListener(new MessageListener<std_msgs.Bool>(){
            @Override
            public void onNewMessage(std_msgs.Bool message){
                if(message.getData() == true){
                    run_reasoner();
                }
            }
        });

        //SPARQLのSELECTを叩くトピックのSubscriber
        Subscriber<ontology_msgs.SPARQLSelectRequest> sub_SPARQL_select_request = connectedNode.newSubscriber("/ontoros/SPARQL_select_request", ontology_msgs.SPARQLSelectRequest._TYPE);
        sub_SPARQL_select_request.addMessageListener(new MessageListener<ontology_msgs.SPARQLSelectRequest>(){
            @Override
            public void onNewMessage(ontology_msgs.SPARQLSelectRequest message){
                run_SPARQL_select(connectedNode, message.getSubject(), message.getPredicate(), message.getObject());
            }
        });

        //インスタンスが属するクラスを追加するトピックのSubscriber
        Subscriber<ontology_msgs.AddClass> sub_add_class = connectedNode.newSubscriber("/ontoros/add_class", ontology_msgs.AddClass._TYPE);
        sub_add_class.addMessageListener(new MessageListener<ontology_msgs.AddClass>(){
            @Override
            public void onNewMessage(ontology_msgs.AddClass message){
                add_class(message.getInstanceUri(), message.getClassUri());
            }
        });

        //インスタンスが属するクラスを削除するトピックのSubscriber
        Subscriber<ontology_msgs.DeleteClass> sub_delete_class = connectedNode.newSubscriber("/ontoros/delete_class", ontology_msgs.DeleteClass._TYPE);
        sub_delete_class.addMessageListener(new MessageListener<ontology_msgs.DeleteClass>(){
            @Override
            public void onNewMessage(ontology_msgs.DeleteClass message){
                delete_class(message.getInstanceUri(), message.getClassUri());
            }
        });

        //インスタンスが属するデータプロパティを追加するトピックのSubscriber
        Subscriber<ontology_msgs.AddDataProperty> sub_add_data_property = connectedNode.newSubscriber("/ontoros/add_data_property", ontology_msgs.AddDataProperty._TYPE);
        sub_add_data_property.addMessageListener(new MessageListener<ontology_msgs.AddDataProperty>(){
            @Override
            public void onNewMessage(ontology_msgs.AddDataProperty message){
                add_data_property(message.getInstanceUri(), message.getPropertyUri(), message.getData());
            }
        });

        //インスタンスが属するデータプロパティを削除するトピックのSubscriber
        Subscriber<ontology_msgs.DeleteDataProperty> sub_delete_data_property = connectedNode.newSubscriber("/ontoros/delete_data_property", ontology_msgs.DeleteDataProperty._TYPE);
        sub_delete_data_property.addMessageListener(new MessageListener<ontology_msgs.DeleteDataProperty>(){
            @Override
            public void onNewMessage(ontology_msgs.DeleteDataProperty message){
                delete_data_property(message.getInstanceUri(), message.getPropertyUri(), message.getData());
            }
        });

        //インスタンスが属するオブジェクトプロパティを追加するトピックのSubscriber
        Subscriber<ontology_msgs.AddObjectProperty> sub_add_object_property = connectedNode.newSubscriber("/ontoros/add_object_property", ontology_msgs.AddObjectProperty._TYPE);
        sub_add_object_property.addMessageListener(new MessageListener<ontology_msgs.AddObjectProperty>(){
            @Override
            public void onNewMessage(ontology_msgs.AddObjectProperty message){
                add_object_property(message.getInstanceUri(), message.getPropertyUri(), message.getObjectUri());
            }
        });

        //インスタンスが属するオブジェクトプロパティを削除するトピックのSubscriber
        Subscriber<ontology_msgs.DeleteObjectProperty> sub_delete_object_property = connectedNode.newSubscriber("/ontoros/delete_object_property", ontology_msgs.DeleteObjectProperty._TYPE);
        sub_delete_object_property.addMessageListener(new MessageListener<ontology_msgs.DeleteObjectProperty>(){
            @Override
            public void onNewMessage(ontology_msgs.DeleteObjectProperty message){
                delete_object_property(message.getInstanceUri(), message.getPropertyUri(), message.getObjectUri());
            }
        });


        //mainループ
        connectedNode.executeCancellableLoop(new CancellableLoop(){
            @Override
            protected void setup(){
                //初期化
                init(connectedNode);
                //run_reasoner();
                //run_SPARQL_select(connectedNode, "<http://www.semanticweb.org/nedo_prius/ontologies/2016/9/untitled-ontology-71#MyCar>", "", "");
            }

            @Override
            protected void loop() throws InterruptedException{
                //System.out.println("[INFO]: This is loop message");
                //Thread.sleep(1000);
            }
        });
    }


    //--------------------------------------------------
    //ROSノード終了時に走る関数
    //--------------------------------------------------
    @Override
    public void onShutdown(Node node){
        System.out.println("[INFO]: Shutdown");
   
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        save_owl_file("owl/" + sdf.format(date.getTime()) + ".owl");

        System.out.println("[INFO]: Shutdown SUCCESS");
    }


    //--------------------------------------------------
    //初期化関数
    //--------------------------------------------------
    static public void init(ConnectedNode connectedNode){
        System.out.println("[INFO]: Init");

        //owlマネージャの初期化
        manager = OWLManager.createOWLOntologyManager();

        //パラメータの情報から主owlファイルにimportされているowlファイルを追加
        ParameterTree params = connectedNode.getParameterTree();
        int imported_owl_num = params.getInteger("/ontoros/imported_owl_num");
        for (int i = 0; i < imported_owl_num; i++){
            manager.addIRIMapper(new SimpleIRIMapper(IRI.create(params.getString("/ontoros/imported_owl_uri/" + (i + 1))), IRI.create(new File(params.getString("/ontoros/imported_owl_path/" + (i + 1))))));
        }

        //主owlファイルの読み込み
        load_owl_file(params.getString("/ontoros/owl_path"));

        factory = manager.getOWLDataFactory();
        reasoner = PelletReasonerFactory.getInstance().createNonBufferingReasoner(ontology);

        System.out.println("[INFO]: Init SUCCESS");
    }


    //--------------------------------------------------
    //owlファイルを読み込む関数
    //--------------------------------------------------
    static public void load_owl_file(String path){
        System.out.println("[INFO]: Load　owl file");

        try{
            ontology = manager.loadOntologyFromOntologyDocument(new File(path));
        }
        catch (OWLOntologyCreationException ex) {
            System.out.println("[ERROR]: Load　owl file FAILE");
            System.out.println(ex);
            System.exit(1);
        }

        System.out.println("[INFO]: Load　owl file SUCCESS");
    }


    //--------------------------------------------------
    //owlファイルを保存する関数
    //--------------------------------------------------
    static public void save_owl_file(String path){
        System.out.println("[INFO]: Save　owl file");        

        try{
            manager.saveOntology(ontology, IRI.create(new File(path)));
        }
        catch (OWLOntologyStorageException ex) {
            System.out.println("[ERROR]: Save　owl file FAILE");
            System.out.println(ex);
            System.exit(1);
        }

        System.out.println("[INFO]: Save　owl file SUCCESS");
    }


    //--------------------------------------------------
    //owlファイルを追加する関数（引数は/hoge/piyo.owl）
    //--------------------------------------------------
    public void add_owl_file(String path){
        OWLOntologyManager manager_ = OWLManager.createOWLOntologyManager();
        //もしimportしているowlがあるなら，ここでaddIRIMapper()を走らせる
        try{
            OWLOntology ontology_ = manager_.loadOntologyFromOntologyDocument(IRI.create(new File(path)));
            for(OWLAxiom axiom : ontology_.getAxioms()){
                manager.addAxiom(ontology, axiom);
	        }
        }
        catch (OWLOntologyCreationException ex) {
            System.out.println("[ERROR]: Add　owl file FAILE");
            System.out.println(ex);
            System.exit(1);
        }                

    }
    

    //--------------------------------------------------
    //owlファイルを削除する関数（引数は/hoge/piyo.owl）
    //--------------------------------------------------
    public void delete_owl_file(String path){
        OWLOntologyManager manager_ = OWLManager.createOWLOntologyManager();
        //もしimportしているowlがあるなら，ここでaddIRIMapper()を走らせる
        try{
            OWLOntology ontology_ = manager_.loadOntologyFromOntologyDocument(IRI.create(new File(path)));        
            for(OWLAxiom axiom : ontology_.getAxioms()){
                manager.removeAxiom(ontology, axiom);
	        }
        }
        catch (OWLOntologyCreationException ex) {
            System.out.println("[ERROR]: Delete　owl file FAILE");
            System.out.println(ex);
            System.exit(1);
        }
    }


    //--------------------------------------------------
    //推論する関数
    //--------------------------------------------------
    static public void run_reasoner(){
        System.out.println("[INFO]: Run reasoner");
        long start = System.currentTimeMillis();
        
        reasoner = PelletReasonerFactory.getInstance().createReasoner(ontology);
        reasoner.getKB().realize();
        List<InferredAxiomGenerator<? extends OWLAxiom>> axiom_generator = new ArrayList<InferredAxiomGenerator<? extends OWLAxiom>>();
        axiom_generator.add(new InferredPropertyAssertionGenerator());
        axiom_generator.add(new InferredClassAssertionAxiomGenerator());
        InferredOntologyGenerator iog = new InferredOntologyGenerator(reasoner, axiom_generator);
        iog.fillOntology(manager, ontology);

        long end = System.currentTimeMillis();
        System.out.println("[INFO]: Run reasoner SUCCESS in " + (end - start)  + " ms");
    }


    //--------------------------------------------------
    //推論結果を検索する関数（SPARQL使用）
    //--------------------------------------------------
    static public void run_SPARQL_select(ConnectedNode connectedNode, String s, String p, String o){
        System.out.println("[INFO]: Run SPARQL select");        
        long start = System.currentTimeMillis();

        reasoner = PelletReasonerFactory.getInstance().createReasoner(ontology);
        reasoner.getKB().realize();      
        kb = reasoner.getKB();

        graph  = new org.mindswap.pellet.jena.PelletReasoner().bind(kb); 
        model = ModelFactory.createInfModel(graph); 

        if(s.length() == 0) s = "?s";
        if(p.length() == 0) p = "?p";
        if(o.length() == 0) o = "?o";

        String query_str = "SELECT ?s ?p ?o WHERE { " + s + " " + p + " " + o + " }";
        System.out.println("[INFO]: Query is " + query_str);
        query = QueryFactory.create(query_str);
        query_exe = SparqlDLExecutionFactory.create(query, model);        
        ResultSet result_set = query_exe.execSelect();
        
        List<String> result_s = new ArrayList<>();
        List<String> result_p = new ArrayList<>();
        List<String> result_o = new ArrayList<>();

        for(; result_set.hasNext(); ){
            QuerySolution solution = result_set.nextSolution();

            if(s == "?s"){
                Resource resource_s = solution.getResource("s");
                result_s.add(resource_s.toString());
            }
            else{
                result_s.add("");
            }

            if(p == "?p"){
                Resource resource_p = solution.getResource("p");
                result_p.add(resource_p.toString());
            }
            else{
                result_p.add("");
            }

            if(o == "?o"){
                Resource resource_o = solution.getResource("o");
                result_o.add(resource_o.toString());
            }
            else{
                result_o.add("");
            }
        }

        long end = System.currentTimeMillis();

        Publisher<ontology_msgs.SPARQLSelectResult> pub_SPARQL_select_result = connectedNode.newPublisher("/ontoros/SPARQL_select_result", ontology_msgs.SPARQLSelectResult._TYPE);
        ontology_msgs.SPARQLSelectResult SPARQL_select_result = connectedNode.getTopicMessageFactory().newFromType(ontology_msgs.SPARQLSelectResult._TYPE);
        SPARQL_select_result.setRow(result_set.getRowNumber());
        SPARQL_select_result.setSubject(result_s);
        SPARQL_select_result.setPredicate(result_p);
        SPARQL_select_result.setObject(result_o);
        pub_SPARQL_select_result.publish(SPARQL_select_result);

        System.out.println("[INFO]: Run SPARQL select SUCCESS in " + (end - start)  + " ms");
    }


    //--------------------------------------------------
    //インスタンスが属するクラスを追加する関数
    //--------------------------------------------------
    static public void add_class(String instance_uri, String class_uri){
        System.out.println("[INFO]: Add class");
        long start = System.currentTimeMillis();
 
        OWLNamedIndividual instance_ = factory.getOWLNamedIndividual(IRI.create(instance_uri)); 
        OWLClassExpression class_ = factory.getOWLClass(IRI.create(class_uri));
        OWLClassAssertionAxiom axiom = factory.getOWLClassAssertionAxiom(class_, instance_); 
        manager.addAxiom(ontology, axiom);

        long end = System.currentTimeMillis();
        System.out.println("[INFO]: Add class SUCCESS in " + (end - start)  + " ms");
    }


    //--------------------------------------------------
    //インスタンスが属するクラスを削除する関数
    //--------------------------------------------------
    static public void delete_class(String instance_uri, String class_uri){
        System.out.println("[INFO]: Delete class");
        long start = System.currentTimeMillis();

        OWLNamedIndividual instance_ = factory.getOWLNamedIndividual(IRI.create(instance_uri)); 
        OWLClassExpression class_ = factory.getOWLClass(IRI.create(class_uri));
        OWLClassAssertionAxiom axiom = factory.getOWLClassAssertionAxiom(class_, instance_); 
        manager.removeAxiom(ontology, axiom);

        long end = System.currentTimeMillis();
        System.out.println("[INFO]: Delete class SUCCESS in " + (end - start)  + " ms");
    }


    //--------------------------------------------------
    //インスタンスが属するデータプロパティをトリプルにより追加する関数
    //--------------------------------------------------
    static public void add_data_property(String instance_uri, String property_uri, String data){
        System.out.println("[INFO]: Add data property");
        long start = System.currentTimeMillis();

        OWLNamedIndividual instance = factory.getOWLNamedIndividual(IRI.create(instance_uri));
        OWLDataProperty property = factory.getOWLDataProperty(IRI.create(property_uri)); 
        OWLDataPropertyAssertionAxiom axiom = factory.getOWLDataPropertyAssertionAxiom(property, instance, data);
        manager.addAxiom(ontology, axiom);

        long end = System.currentTimeMillis();
        System.out.println("[INFO]: Add data property SUCCESS in " + (end - start)  + " ms");
    }


    //--------------------------------------------------
    //インスタンスが属するデータプロパティをトリプルにより削除する関数
    //--------------------------------------------------
    static public void delete_data_property(String instance_uri, String property_uri, String data){
        System.out.println("[INFO]: Delete data property");
        long start = System.currentTimeMillis();

        OWLNamedIndividual instance = factory.getOWLNamedIndividual(IRI.create(instance_uri));
        OWLDataProperty property = factory.getOWLDataProperty(IRI.create(property_uri)); 
        OWLDataPropertyAssertionAxiom axiom = factory.getOWLDataPropertyAssertionAxiom(property, instance, data);
        manager.removeAxiom(ontology, axiom);

        long end = System.currentTimeMillis();
        System.out.println("[INFO]: Delete data property SUCCESS in " + (end - start)  + " ms");
    }


    //--------------------------------------------------
    //インスタンスが属するオブジェクトプロパティをトリプルにより追加する関数
    //--------------------------------------------------
    static public void add_object_property(String instance_uri, String property_uri, String object_uri){
        System.out.println("[INFO]: Add object property");
        long start = System.currentTimeMillis();

        OWLNamedIndividual instance = factory.getOWLNamedIndividual(IRI.create(instance_uri));
        OWLObjectProperty property = factory.getOWLObjectProperty(IRI.create(property_uri));
        OWLNamedIndividual object = factory.getOWLNamedIndividual(IRI.create(object_uri));
        OWLObjectPropertyAssertionAxiom axiom = factory.getOWLObjectPropertyAssertionAxiom(property, instance, object);
        manager.addAxiom(ontology, axiom);

        long end = System.currentTimeMillis();
        System.out.println("[INFO]: Add object property SUCCESS in " + (end - start)  + " ms");
    }


    //--------------------------------------------------
    //インスタンスが属するオブジェクトプロパティをトリプルにより削除する関数
    //--------------------------------------------------
    static public void delete_object_property(String instance_uri, String property_uri, String object_uri){
        System.out.println("[INFO]: Delete object property");
        long start = System.currentTimeMillis();

        OWLNamedIndividual instance = factory.getOWLNamedIndividual(IRI.create(instance_uri));
        OWLObjectProperty property = factory.getOWLObjectProperty(IRI.create(property_uri));
        OWLNamedIndividual object = factory.getOWLNamedIndividual(IRI.create(object_uri));
        OWLObjectPropertyAssertionAxiom axiom = factory.getOWLObjectPropertyAssertionAxiom(property, instance, object);
        manager.removeAxiom(ontology, axiom);

        long end = System.currentTimeMillis();
        System.out.println("[INFO]: Delete object property SUCCESS in " + (end - start)  + " ms");
    }
}
