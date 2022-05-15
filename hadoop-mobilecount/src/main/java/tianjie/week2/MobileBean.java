package tianjie.week2;

import java.io.DataInput;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Writable;

public class MobileBean implements  Writable{
    private String mobileNum;
    private String upLink;
    private String downLink;
    private String total;
    public MobileBean(){

}
    private String getFieldValue(String fieldValue) {
        return StringUtils.isBlank(fieldValue) ? "" : fieldValue;
    }
    public String getUpLink() {return this.upLink;}
    public void setUpLink(String upLink_in) {this.upLink =upLink_in;}
    public String getDownLink() {return this.downLink;}
    public void setDownLink(String downLink_in) {this.downLink=downLink_in;}
    public String getMobileNum() {return this.mobileNum;}
    public void setMobileNum(String mobileNum_in) {this.mobileNum = mobileNum_in;}
    public String getTotal() {return this.total;}
    public void setTotal(String total_in){this.total = total_in;}

    public void write(DataOutput dataOutput) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.getFieldValue(this.mobileNum)).append(",");
        stringBuilder.append(this.getFieldValue(this.upLink)).append(",");
        stringBuilder.append(this.getFieldValue(this.downLink)).append(",");
        stringBuilder.append(this.getFieldValue(this.total));
        dataOutput.writeUTF(stringBuilder.toString());
    }

    public void readFields(DataInput dataInput) throws IOException {
        String line = dataInput.readUTF();
        System.out.println(line);
        String[] valueArray = line.split(",");
        this.mobileNum = valueArray[0];
        this.upLink = valueArray[1];
        this.downLink = valueArray[2];
    }

    public String toString() {
        return this.upLink + '\t' + " " + this.downLink + '\t' + " " + this.total;
    }

}
