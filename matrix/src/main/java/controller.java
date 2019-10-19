import java.util.Arrays;

import multiply.MatrixMultiply;
import relation.*;

public class controller {
    static void help()
    {
        System.err
                .println("Usage: \n\tmul <inputPathM> <inputPathN> <outputPath>" +
                        "\n\tselect <inputPath> <outputPath> <Column name> <operator> <value>" +
                        "\n\tproj <inputPath> <outputPath> <Column name>" +
                        "\n\tinter <inputPath1> <inputPath2> <outputPath>" +
                        "\n\tunion <inputPath1> <inputPath2> <outputPath>" +
                        "\n\tdiff <inputPath1> <inputPath2> <outputPath>" +
                        "\n\tnatural <inputPath1> <inputPath2> <outputPath> <Column name>");
        System.exit(2);
    }

    public static void main(String[] args) throws Exception {
        if(args.length==0)
        {
            help();
        }else if(args[0].equals("mul"))
        {
            MatrixMultiply.main(Arrays.copyOfRange(args,1,args.length));
        }else if(args[0].equals("select")&&args.length==6)
        {
            Selection.main(Arrays.copyOfRange(args,1,args.length));
        }else if(args[0].equals("proj")&&args.length==4)
        {
            Projection.main(Arrays.copyOfRange(args,1,args.length));
        }else if(args[0].equals("inter")&&args.length==4)
        {
            Intersection.main(Arrays.copyOfRange(args,1,args.length));
        }else if(args[0].equals("union")&&args.length==4)
        {
            Union.main(Arrays.copyOfRange(args,1,args.length));
        }else if(args[0].equals("diff")&&args.length==4)
        {
            Difference.main(Arrays.copyOfRange(args,1,args.length));
        }else if(args[0].equals("natural")&&args.length==5)
        {
            NaturalJoin.main(Arrays.copyOfRange(args,1,args.length));
        }else
        {
            help();
        }
    }

}
