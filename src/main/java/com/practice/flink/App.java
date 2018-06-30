package com.practice.flink;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.flink.filter.AvgMapFunction;
import com.practice.flink.filter.Converters;
import com.practice.flink.filter.TimeStampAssigner;
import com.practice.flink.model.ReceivedData;
import com.practice.flink.model.SensorData;
import com.practice.flink.source.MqttFlinkSource;



/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
       //final StreamExecutionEnvironment environment=StreamExecutionEnvironment.createLocalEnvironment();
       final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setStateBackend(new MemoryStateBackend());
        CheckpointConfig config = environment.getCheckpointConfig();
        environment.enableCheckpointing(1000);
        
        
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
       // FlinkKafkaConsumer011<String> consumer=new FlinkKafkaConsumer011<String>("flink-test", new SimpleStringSchema(),properties);
        MqttFlinkSource mqttconsumer=new MqttFlinkSource("tcp://localhost:1883","flink-client","/flink");
        
        DataStream<MqttMessage> mqttStream = environment.addSource(mqttconsumer);//mqtt siurce added 
        //DataStream<String> kafkastream = environment.addSource(consumer);//kafka stream added
   
        final OutputTag<String> errorTag = new OutputTag<String>("Error"){}; //create a error side stream to have error data in this
       
        /*
        //two stream connected without changing there type in flatmap function we change both to string
       DataStream<String> stream=kafkastream.connect(mqttStream).flatMap(new CoFlatMapFunction<String, MqttMessage, String>() {
		private static final long serialVersionUID = 1L;
		@Override
		public void flatMap1(String value, Collector<String> out) throws Exception {
			out.collect(value);
		}
		@Override
		public void flatMap2(MqttMessage value, Collector<String> out) throws Exception {
			out.collect( new String(value.getPayload(), "UTF8"));
		}
       });*/
        
        DataStream<String> stream=mqttStream.flatMap(new FlatMapFunction<MqttMessage,String>() {

			@Override
			public void flatMap(MqttMessage value, Collector<String> out) throws Exception {
				out.collect( new String(value.getPayload(), "UTF8"));
				
			}
		});
        
        
        //this will check the json message is valid or not if not pass to error stream or to mainstream
        SingleOutputStreamOperator<String> mainDataStream = stream.process(new ProcessFunction<String,String>() {
			@Override
			public void processElement(String value, ProcessFunction<String,String>.Context context, Collector<String> out) {
				try {
				ObjectMapper mapper=new ObjectMapper();
				ReceivedData data=mapper.readValue(value, ReceivedData.class);
				out.collect(value);
				}catch(Exception e) {
					context.output(errorTag,String.valueOf("Data:"+value+"  Exception:"+e.getMessage()));
				}
			}
		});

        
        
        DataStream<ReceivedData> datastream = mainDataStream.flatMap(new Converters().new JsonToReceiveData());
        DataStream<ReceivedData> datastreamwithtime=datastream.assignTimestampsAndWatermarks(new TimeStampAssigner());
        DataStream<SensorData> sensorDataStream=datastreamwithtime.flatMap(new Converters().new JsonToSensorData());
        
        DataStream<String> errorstream=mainDataStream.getSideOutput(errorTag);//get the side datastream from mainstream
        
        errorstream.print();
        sensorDataStream.keyBy("key").map(new AvgMapFunction()).print();
		environment.execute();
    }
}
