package wang.wincent.techstack.storm.example.logAnalyze.app.callback;


import wang.wincent.techstack.storm.example.logAnalyze.app.domain.BaseRecord;
import wang.wincent.techstack.storm.example.logAnalyze.storm.dao.LogAnalyzeDao;
import wang.wincent.techstack.storm.example.logAnalyze.storm.utils.DateUtils;

import java.util.Calendar;
import java.util.List;

/**
 * Describe: 计算每天的全量数据
 * Data:     2015/11/17.
 */
public class DayAppendCallBack implements Runnable{
    @Override
    public void run() {
        Calendar calendar = Calendar.getInstance();
        if (calendar.get(Calendar.MINUTE) == 0 && calendar.get(Calendar.HOUR) == 0) {
            String endTime = DateUtils.getDataTime(calendar);
            String startTime = DateUtils.beforeOneDay(calendar);
            LogAnalyzeDao logAnalyzeDao = new LogAnalyzeDao();
            List<BaseRecord> baseRecordList = logAnalyzeDao.sumRecordValue(startTime, endTime);
            logAnalyzeDao.saveDayAppendRecord(baseRecordList);
        }
    }
}