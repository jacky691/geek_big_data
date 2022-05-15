package tianjie.week2;

import java.io.IOException;
import java.util.Iterator;


import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MobileReduce extends Reducer<Text, MobileBean, Text, MobileBean> {
    public MobileReduce() {
    }

    protected void reduce(Text key, Iterable<MobileBean> values, Reducer<Text, MobileBean, Text, MobileBean>.Context context) throws IOException, InterruptedException {
        Iterator var4 = values.iterator();
        MobileBean bean = new MobileBean();
        Integer totalUp = 0;

        Integer totalDown;
        MobileBean value;
        for(totalDown = 0; var4.hasNext(); totalDown = totalDown + this.getIntegerValue(value.getDownLink())) {
            value = (MobileBean)var4.next();
            bean.setMobileNum(value.getMobileNum());
            totalUp = totalUp + this.getIntegerValue(value.getDownLink());
        }

        bean.setUpLink(String.valueOf(totalUp));
        bean.setDownLink(String.valueOf(totalDown));
        bean.setTotal(String.valueOf(totalUp + totalDown));
        context.write(key, bean);
    }

    private Integer getIntegerValue(String value) {
        return StringUtils.isBlank(value) ? 0 : Integer.valueOf(value);
    }
}
