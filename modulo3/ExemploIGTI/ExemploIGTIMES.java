
package IGTI;

import java.io.*;
import java.util.*;
import java.util.Random;
import java.text.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class ExemploIGTI extends Configured implements Tool 
{          
    public static void main (final String[] args) throws Exception {   
      int res = ToolRunner.run(new Configuration(), new ExemploIGTI(), args);        
      System.exit(res);           
    }   

    public int run (final String[] args) throws Exception {
      try{ 	             	       	
            JobConf conf = new JobConf(getConf(), ExemploIGTI.class);
            conf.setJobName("Calculo Covid19");

            final FileSystem fs = FileSystem.get(conf);
  
            Path diretorioEntrada = new Path("PastaEntrada"), diretorioSaida = new Path("PastaSaida");
            fs.mkdirs(diretorioEntrada);

            fs.copyFromLocalFile(new Path("/usr/local/hadoop/Dados/covidData.txt"), diretorioEntrada);

            FileInputFormat.setInputPaths(conf, diretorioEntrada);
            FileOutputFormat.setOutputPath(conf, diretorioSaida);

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            conf.setMapperClass(MapIGTI.class);
            conf.setReducerClass(ReduceIGTI.class);
            JobClient.runJob(conf);            
        }
        catch ( Exception e ) {
            throw e;
        }
        return 0;
     }
 
    public static class MapIGTI extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
            
      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)  throws IOException {
           Text txtChave = new Text();
           Text txtValue = new Text();
           
           String[] dadosCovid = value.toString().split(",");
           String mesEvento = dadosCovid[0].substring(5, 7);
           String paisEvento = dadosCovid[2];
           int novosCasos = Integer.parseInt(dadosCovid[4]);
           int novosObitos = Integer.parseInt(dadosCovid[6]);

           String strChave = mesEvento + "|" + paisEvento;
           String strValor = String.valueOf(novosCasos) + "|" + String.valueOf(novosObitos);

           txtChave.set(strChave);
           txtValue.set(strValor);
           output.collect(txtChave, txtValue);            
      }        
    }
 
   
    public static class ReduceIGTI extends MapReduceBase implements Reducer<Text, Text, Text, Text> {       
       public void reduce (Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {   
           int somaCasos = 0, somaObitos = 0;
          
           Text value = new Text();
           String[] campos = new String[2];
           String[] camposChave = new String[2];
           String strSaida = "";
       
           camposChave = key.toString().split("\\|");

           while (values.hasNext()) {
              value = values.next();   
              campos = value.toString().split("\\|");
              somaCasos += Integer.parseInt(campos[0]);                              
              somaObitos += Integer.parseInt(campos[1]);             
           }
           strSaida = "Mes: " + camposChave[0] + " em " + camposChave[1] + ":";
           strSaida += "Total casos:: " + String.valueOf(somaCasos);
           strSaida += "Total obitos: " + String.valueOf(somaObitos);

           value.set(strSaida);
           output.collect(key, value);           
      }                
    }

   public static class MapIGTIMaior extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)  throws IOException {

           
     }
}

    public static class ReduceIGTIMaior extends MapReduceBase implements Reducer<Text, Text, Text, Text> {   
       public void reduce (Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {      
         
             
  }
}

}

