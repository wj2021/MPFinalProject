//package pre;
//
//import java.io.*;
//
//// 生成json数据在高德地图API中可以显示地理位置点的分布
//public class Test {
//    public static void main(String[] args) throws IOException {
//        FileReader fr = new FileReader(new File("output/part-00001"));
//        BufferedReader br = new BufferedReader(fr);
//        String line;
//        StringBuffer ans = new StringBuffer("var markers = [\n");
//        while((line=br.readLine()) != null) {
//            String[] tokens = line.split(",");
//            if("4".equals(tokens[2])) {
//                ans.append("{icon: '//a.amap.com/jsapi_demos/static/demo-center/icons/poi-marker-1.png',position: ["+tokens[1]+", "+tokens[0]+"]},\n");
//            }
//        }
//        ans.replace(ans.length()-2, ans.length(), "\n];");
//        System.out.println(ans);
//    }
//}
