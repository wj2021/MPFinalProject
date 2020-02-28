//package pre;
//
//import java.io.*;
//import java.util.Objects;
//
///**
// * 仅能处理本地文件
// * 预处理轨迹数据，删除前6行无用数据并只保留经纬度和时间信息, 把用户的一条轨迹信息转换成一行保存起来（源数据一条轨迹使用一个文件保存）
// * 控制台参数 args[0]:原始轨迹数据目录 args[1]:处理后轨迹数据保存目录
// */
//public class InputPreprocess {
//    public static void main(String[] args) throws IOException {
//        // 如果输出目录存在则删除,再创建目录
//        deleteFileRecursion(new File(args[1]));
//        File f = new File(args[1]);
//        f.mkdir();
//
//        // 读取原始轨迹数据，处理后保存到输出目录中去
//        File data = new File(args[0]);
//        StringBuilder outStr = new StringBuilder();
//        final int SIZE_THRESH = 52428800; // 写入文件的大小阈值
//        int fileCount = 0; // 记录生成了多少个输出文件
//        File[] trajectoryDirs = data.listFiles();
//        assert trajectoryDirs != null;
//
//        for(int i = 0; i < trajectoryDirs.length; ++i) {
//            File f1 = trajectoryDirs[i]; // f1: 000目录
//            for(File f2 : Objects.requireNonNull(f1.listFiles())) {
//                if(f2.isDirectory()) { // f2: Trajectory目录
//                    StringBuilder sb = new StringBuilder();
//                    for(File f3 : Objects.requireNonNull(f2.listFiles())) { // plt轨迹文件
//                        sb.append(f1.getName()).append("\t"); // 每条轨迹前面加上用户名
//                        FileReader fr = new FileReader(f3);
//                        BufferedReader br = new BufferedReader(fr);
//                        String line;
//                        for(int k=0; k<6; ++k) br.readLine(); // 原始轨迹数据中前6行无用
//                        while((line = br.readLine()) != null) {
//                            String[] tokens = line.split(",");
//                            // 只保留 纬度,经度,日期 时间 信息
//                            sb.append(tokens[0]).append(",").append(tokens[1]).append(",").append(tokens[5]).append(" ").append(tokens[6]).append("->");
//                        }
//                        sb.replace(sb.length()-2, sb.length(), "\n");
//                        br.close();fr.close();
//                    }
//                    // 将多个用户的轨迹数合并起来，防止生成一堆小文件
//                    outStr.append(sb);
//                }
//            }
//            // 当读取的内容大小超过阈值或者文件已经读完了，将结果写入到文件中去
//            if(outStr.length() > SIZE_THRESH || i == trajectoryDirs.length-1) {
//                File newFile = new File(args[1] + File.separator + fileCount + ".txt");
//                BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(newFile));
//                outputStream.write(outStr.toString().getBytes());
//                outputStream.close();
//                fileCount ++;
//                outStr = new StringBuilder();
//            }
//        }
//    }
//
//    private static void deleteFileRecursion(File file) {
//        if(file.exists()) {
//            if(file.isDirectory()) {
//                File[] files = file.listFiles();
//                if(files != null){
//                    for(File f : files) {
//                        deleteFileRecursion(f);
//                    }
//                }
//            } else {
//                file.delete();
//            }
//        }
//    }
//}
