package custom;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ACGroupComparator extends WritableComparator {
    public ACGroupComparator() {
        super(Text.class, true);
    }
    @Override
    public int compare(WritableComparable acText1, WritableComparable acText2) {
        String[] a_c1 = acText1.toString().split(",");
        String a1 = a_c1[0];

        String[] a_c2 = acText2.toString().split(",");
        String a2 = a_c2[0];

        Text a1Text = new Text(a1);
        Text a2Text = new Text(a2);

        return a1Text.compareTo(a2Text);


//        TemperaturePair temperaturePair = (TemperaturePair) tp1;
//        TemperaturePair temperaturePair2 = (TemperaturePair) tp2;
//        return temperaturePair.getYearMonth().compareTo(temperaturePair2.getYearMonth());
    }
}