package com.wanmei.gamelog.run;

import com.wanmei.gamelog.ETLFactory.ETLAopFactory;
import com.wanmei.gamelog.common.ParameterBean;
import com.wanmei.gamelog.etl.CommonETLImpl;
import com.wanmei.gamelog.etl.GameETL;
import com.wanmei.gamelog.unit.ETLUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: liliangyang
 * Date: 14-6-23
 * Time: 下午5:37
 * 用来解析每小时的log
 */
public class GameLogHourETLRun {

    public static class HourETLMap extends Mapper<Objects, Text, Text, NullWritable> {
        // map输出的关键字
        private Text mapKey = new Text();
        ParameterBean parameterBean = null;//用来存放etl解析器的参数


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //setup方法会在每个map执行前执行一次。初始化此map执行解析方法etl的参数
            parameterBean = new ParameterBean();

            // 游戏配置文件的路径 hdfs://master:49000/user/hadoop/etl/conf/xa.properties
            //在调用main方法是传入这个参数
            Properties properties = new Properties();
            String gamePropertiesPath = context.getConfiguration().get("game.properties.path");
            Configuration conf = context.getConfiguration();
            Path path = new Path(gamePropertiesPath);
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            InputStream inputStream = fileSystem.open(path);
            properties.load(inputStream);


            //读取log的文件名，得到文件名后就能得到解析这个log的正则表达式。
            // 读取块,取得当前正在处理的log文件的名称
            InputSplit inputSplit = context.getInputSplit();
            // 获得读取文件的路径
            //eg：filePath=hdfs://master:49000/export/gamelog/xa/2014-05-29/xa.new_rolesbrief.csv.1.03
            String logFilePathAndName = ((FileSplit) inputSplit).getPath().toString();
            System.out.println("logFilePathAndName=" + logFilePathAndName);
            parameterBean.setLogFilePathAndName(logFilePathAndName);

            String logFileName= ETLUnit.getLogFileNameFromPath(logFilePathAndName);
            parameterBean.setLogFileName(logFileName);

            String day=ETLUnit.getLogFileDayFromPath(logFilePathAndName);
            parameterBean.setDay(day);

            String hour=ETLUnit.getLogFileHourFromPath(logFilePathAndName);
            parameterBean.setHour(hour);

            //一个log文件只有一种格式，因此只要一种正则表达式就行了
            String logPattern = properties.getProperty(parameterBean.getLogFileName() + ".pattern");
             parameterBean.setPattern(logPattern);

             //取得解析当前log的ETL实现类地址
            String etlClassName = properties.getProperty("etlClassName");
             parameterBean.setEtlClassName(etlClassName);

        }

        @Override
        protected void map(Objects key, Text value, Context context) throws IOException, InterruptedException {
            parameterBean.setLine(value.toString().trim());

            StringBuffer lineByEtled = null;//存放被etl方法解析之后的数据，是被解析文件中的一行数据。
            GameETL etl = null;//解析器，注意这里是接口;


            //开始解析每一行数据
            etl = (CommonETLImpl) ETLAopFactory.getAOPProxyedObject(parameterBean.getEtlClassName());
            lineByEtled = etl.execute(parameterBean);

            //将解析之后的一行数据作为Map输出的key，value为空就行。
            if (null != lineByEtled && lineByEtled.toString().trim().length() > 0) {
                mapKey.set(lineByEtled.toString());
                context.write(mapKey, NullWritable.get());
            }

        }
    }


    public static class HourETLReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }


    public static void main(String[] args) throws Exception {
        String logFilePathAndName="";
        String logFileName=logFilePathAndName.substring(0,logFilePathAndName.length()-4);
    }
}
