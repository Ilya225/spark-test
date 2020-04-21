package spark.test.app.jobs;

import spark.test.app.AppConf;

import java.io.Serializable;

public interface JobRunner extends Serializable {
    void run();
    AppConf getAppConf();
}
