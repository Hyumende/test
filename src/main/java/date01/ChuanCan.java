package date01;

import org.apache.flink.api.java.utils.ParameterTool;

public class ChuanCan {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if(parameterTool.has("input")){
            String input = parameterTool.get("input");
            System.out.println(input);
        }
    }

}
