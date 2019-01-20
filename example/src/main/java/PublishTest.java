
import com.qcloud.cmq.client.exception.MQClientException;
import com.qcloud.cmq.client.exception.MQServerException;
import com.qcloud.cmq.client.producer.Producer;
import com.qcloud.cmq.client.producer.PublishCallback;
import com.qcloud.cmq.client.producer.PublishResult;

import java.util.ArrayList;
import java.util.List;

public class PublishTest {
    public static void main(String args[]) {

        Producer producer = new Producer();
        producer.setSecretId("AKIDrcs5AKw5mKiutg9gxGOK5eEdppfLw7D7");
        producer.setSecretKey("qV2NVSEPRGtMfqpcCaoqhMH14wc6TgiY");
        producer.setNameServerAddress("http://cmq-nameserver-dev.api.qcloud.com");
        String route_topic = "lambda-topic";
        String tag_topic = "lambda-topic";

        try {
            producer.start();
            while (true) {
                String msg = "publish SYNC msg with routeKey:aa";
                PublishResult result = producer.publish(route_topic, msg, "aa");
                System.out.println("==> publish msg:" + msg + " result:" + result);

                msg = "publish SYNC msg with tag bb and cc";
                List<String> tagList = new ArrayList<String>();
                tagList.add("bb");
                tagList.add("cc");
                result = producer.publish(tag_topic, msg, tagList);
                System.out.println("==> publish msg:" + msg + " result:" + result);

                final String routeMsg = "publish ASYNC msg with routeKey:dd ";
                producer.publish(route_topic, routeMsg, "dd", new PublishCallback() {
                    @Override
                    public void onSuccess(PublishResult publishResult) {
                        System.out.println("==> publish msg:" + routeMsg + " result:" + publishResult);
                    }

                    @Override
                    public void onException(Throwable e) {
                        System.out.println("==> publish msg:" + routeMsg + " failed with error:" + e);
                    }
                });
                final String tagMsg = "publish ASYNC msg with tag ee and ff";
                tagList.clear();
                tagList.add("ee");
                tagList.add("ff");
                producer.publish(tag_topic, tagMsg, tagList, new PublishCallback() {
                    @Override
                    public void onSuccess(PublishResult publishResult) {
                        System.out.println("==> publish msg:" + tagMsg + " result:" + publishResult);
                    }

                    @Override
                    public void onException(Throwable e) {
                        System.out.println("==> publish msg:" + tagMsg + " failed with error:" + e);
                    }
                });
            }
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (MQServerException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(1000);
            producer.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
