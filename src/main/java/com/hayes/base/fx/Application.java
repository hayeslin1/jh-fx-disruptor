package com.hayes.base.fx;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

/**
 * 应用启动类
 * <p>
 * 作用：Spring Boot 工程入口，加载自动配置并启动内嵌容器。
 * 包扫描：默认扫描 {@code com.hayes.base.fx} 及其子包；
 * {@link MapperScan} 明确扫描 MyBatis Mapper 接口包。
 */
@SpringBootApplication
@MapperScan("com.hayes.base.fx.mapper")
public class Application {

    /**
     * 主入口
     *
     * @param args 启动参数，可通过 --spring.profiles.active=xxx 指定激活环境
     */
    public static void main(String[] args) {
        // 启动 Spring 容器，返回上下文用于打印启动信息
        ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);
        Environment env = context.getEnvironment();
        // 读取当前服务端口与激活 profile，便于启动后直接定位服务地址
        String port = env.getProperty("server.port", "8080");
        String contextPath = env.getProperty("server.servlet.context-path", "");
        String[] profiles = env.getActiveProfiles();
        System.out.println("\n----------------------------------------------------------");
        System.out.println("  Application '" + env.getProperty("spring.application.name") + "' is running!");
        System.out.println("  Local:   http://localhost:" + port + contextPath);
        System.out.println("  Profile: " + (profiles.length == 0 ? "default" : String.join(",", profiles)));
        System.out.println("----------------------------------------------------------\n");
    }
}
