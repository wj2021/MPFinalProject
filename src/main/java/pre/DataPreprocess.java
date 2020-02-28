package pre;

import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * 预处理 HDFS 上的文件
 * 预处理轨迹数据，删除前6行无用数据并只保留经纬度和时间信息, 把用户的一条轨迹信息转换成一行保存起来（源数据一条轨迹使用一个文件保存）
 * 控制台参数 args[0]:原始轨迹数据目录 args[1]:处理后轨迹数据保存目录
 */
public class DataPreprocess {
    public static void processing(String src, String dst, FileSystem dfs) throws IOException {
        // 如果输出目录存在直接返回
        Path outPath = new Path(dst);
        if(dfs.exists(outPath)) {
//            dfs.delete(outPath, true);
//            dfs.mkdirs(outPath);
            return;
        }

        final int SIZE_THRESH = 52428800; // 写入文件的大小阈值下限
        int fileCount = 0; // 记录生成了多少个输出文件

        StringBuffer outStr = new StringBuffer();
        FSDataInputStream fsi;
        BufferedReader br;
        String line;

        Path dataPath = new Path(src);
        if(dfs.exists(dataPath) && dfs.isDirectory(dataPath)) {
            FileStatus[] userFiles =  dfs.listStatus(dataPath);
            for(int i=0; i<userFiles.length; ++i) {
                Path userPath = userFiles[i].getPath(); // 000目录
                if(userFiles[i].isDirectory()) {
                    FileStatus[] tmpFiles = dfs.listStatus(userPath);
                    for(FileStatus tfs : tmpFiles) {
                        if(tfs.isDirectory()) {
                            FileStatus[] trajectoryFiles = dfs.listStatus(tfs.getPath());
                            for(FileStatus traFs : trajectoryFiles) {
                                outStr.append(userPath.getName()).append("\t"); // 每条轨迹数据前面加上用户名
                                fsi = dfs.open(traFs.getPath()); // 打开一条轨迹文件
                                br = new BufferedReader(new InputStreamReader(fsi));
                                for(int k=0;k<6;++k) br.readLine(); // 原始轨迹数据中前6行无用
                                while((line=br.readLine())!=null) {
                                    String[] tokens = line.split(",");
                                    // 只保留 纬度,经度,日期 时间 信息
                                    outStr.append(tokens[0]).append(",").append(tokens[1]).append(",")
                                            .append(tokens[5]).append(" ").append(tokens[6]).append("->");
                                }
                                outStr.replace(outStr.length()-2, outStr.length(), "\n");
                                br.close();fsi.close();
                            }
                        }
                    }
                }

                // 当读取的内容大小超过阈值或者文件已经读完了，将结果写入到 HDFS 中去
                if(outStr.length() > SIZE_THRESH || i == userFiles.length-1) {
                    FSDataOutputStream out = dfs.create(new Path(dst+"/"+fileCount+".txt"));
                    out.write(outStr.toString().getBytes(StandardCharsets.UTF_8));
                    out.close();
                    fileCount++;
                    outStr = new StringBuffer();
                }
            }
        }
    }
}
