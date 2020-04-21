package Yz.Monitor.Contract;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import sun.misc.Signal;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class MonitorContract4Mail {
    private final String topic = "topic";
    private final String receipt = "receipt";
    private final String title = "title";
    private final String subject = "subject";
    private final String content = "content";
    private Properties prop = new Properties();
    private RedisClient redisClient = null;
    private RedisCommands<String, String> syncCommand = null;
    private StatefulRedisConnection<String, String> connection = null;
    private StatefulRedisPubSubConnection<String, String> subConnection = null;
    private StatefulRedisPubSubConnection<String, String> pubConnection = null;
    private RedisAsyncCommands<String, String> asyncCommand = null;
    private RedisPubSubCommands<String, String> syncSubCommand = null;
    private RedisPubSubCommands<String, String> syncPubCommand = null;
    private RedisPubSubListener<String, String> subListener = null;
    private String[] topics = null;
    private InternetAddress[] receipts = null;

    public static void main(String[] args) throws InterruptedException {
        MonitorContract4Mail mail = new MonitorContract4Mail();
        CountDownLatch latch = new CountDownLatch(1);
        Signal.handle(new Signal("INT"), signal -> {
            latch.countDown();
        });
        mail.createRedisClient();
        mail.subscriptRedisMessage();
        try {
            latch.await();
        } finally {
            mail.releaseRedisClient();
        }
    }

    public MonitorContract4Mail() {
        try (InputStream in = new BufferedInputStream(new FileInputStream("config.ini"))) {
            prop.load(in);
            if (!prop.containsKey(topic)) {
                System.out.println("No subscript topic");
                System.exit(0);
            }
            if (!prop.containsKey(receipt)) {
                System.out.println("No receipt Email address");
                System.exit(0);
            }
        } catch (Exception e) {
            System.out.println("read config.ini error");
        }
    }

    private void createRedisClient() {
        RedisURI.Builder builder = RedisURI.builder().withHost(prop.getProperty("host"));
        if (prop.containsKey("port")) {
            builder.withPort(Integer.parseInt(prop.getProperty("port")));
        }
        if (prop.containsKey("password")) {
            builder.withPassword(prop.getProperty("password"));
        }
        redisClient = RedisClient.create(builder.build());
        connection = redisClient.connect();
        subConnection = redisClient.connectPubSub();
        pubConnection = redisClient.connectPubSub();
        syncSubCommand = subConnection.sync();
    }

    private void releaseRedisClient() {
        if (syncSubCommand != null) {
            syncSubCommand.unsubscribe(topics);
            syncSubCommand.quit();
        }
        if (syncPubCommand != null) {
            syncPubCommand.quit();
        }
        if (syncCommand != null) {
            syncCommand.quit();
        }
        if (asyncCommand != null) {
            asyncCommand.quit();
        }
        if (subConnection != null && subListener != null) {
            subConnection.removeListener(subListener);
        }
        if (redisClient != null) {
            if (connection != null) {
                connection.close();
            }
            if (subConnection != null) {
                subConnection.close();
            }
            if (pubConnection != null) {
                pubConnection.close();
            }
            redisClient.shutdown();
            redisClient = null;
        }
    }

    private void subscriptRedisMessage() {
        if (prop.containsKey(topic)) {
            JSONArray arrayTopic = JSONArray.parseArray(prop.getProperty(topic, "[]").toLowerCase());
            topics = new String[arrayTopic.size()];
            arrayTopic.toJavaList(String.class).toArray(topics);
        }
        if (prop.containsKey(receipt)) {
            List<InternetAddress> addressList = new ArrayList<>();
            JSONArray.parseArray(prop.getProperty(receipt, "[]")).forEach(a -> {
                try {
                    addressList.add(new InternetAddress((String) a));
                } catch (AddressException e) {
                    e.printStackTrace();
                }
            });
            receipts = new InternetAddress[addressList.size()];
            addressList.toArray(receipts);
        }
        subListener = new RedisPubSubListener<String, String>() {
            @Override
            public void message(String s, String s2) {
                System.out.println(String.format("%s: topic:%s,content:%s", LocalDateTime.now().toString(), s, s2));
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s2);
                    if (jsonObject.containsKey(title) && jsonObject.containsKey(subject) && jsonObject.containsKey(content)) {
                        sendMail(jsonObject.getString(title), jsonObject.getString(subject), jsonObject.getString(content), receipts);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                }
            }

            @Override
            public void message(String s, String k1, String s2) {

            }

            @Override
            public void subscribed(String s, long l) {
                System.out.println("Redis subscribed: " + s + ", index:" + l);
            }

            @Override
            public void psubscribed(String s, long l) {

            }

            @Override
            public void unsubscribed(String s, long l) {
                System.out.println("Redis unsubscribed: " + s + ", index:" + l);
            }

            @Override
            public void punsubscribed(String s, long l) {

            }
        };
        subConnection.addListener(subListener);
        syncSubCommand.subscribe(topics);
    }

    private void sendMail(String title, String subject, String content, InternetAddress[] receiptAddress) {
        try {
            Properties properties = new Properties();
            properties.put("mail.transport.protocol", "smtp");// 连接协议
            properties.put("mail.smtp.host", "smtp.126.com");// 主机名
            properties.put("mail.smtp.port", 465);// 端口号
            properties.put("mail.smtp.auth", "true");
            properties.put("mail.smtp.ssl.enable", "true");// 设置是否使用ssl安全连接 ---一般都使用
            String mailAddress = "StatusMonitor@126.com";
            String password = "Statusmonitor123";
//        properties.put("mail.debug", "true");// 设置是否显示debug信息 true 会在控制台显示相关信息
            // 得到回话对象
            Session session = Session.getInstance(properties);
            // 获取邮件对象
            Message message = new MimeMessage(session);
            // 设置发件人邮箱地址
            message.setFrom(new InternetAddress(mailAddress, title));
            // 设置收件人邮箱地址
            message.setRecipients(Message.RecipientType.TO, receiptAddress); //多个收件人
//            message.setRecipient(Message.RecipientType.TO, new InternetAddress(receiptAddress));//一个收件人
            // 设置邮件标题
            message.setSubject(subject);
            // 设置邮件内容
            message.setText(content);
            // 得到邮差对象
            Transport transport = session.getTransport();
            // 连接自己的邮箱账户
            transport.connect(mailAddress, password);// 密码为126邮箱开通的stmp服务后得到的客户端授权码
            // 发送邮件
            transport.sendMessage(message, message.getAllRecipients());
            transport.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
}
