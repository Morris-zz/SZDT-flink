package org.example.etl.source;

import cn.hutool.core.io.FileUtil;
import cn.hutool.http.HttpUtil;

import static org.example.util.Constant.ORIGINAL_DATA;

/**
 * @author Geek
 * @date 2020-04-13 19:11:04
 *
 * 由于在前期保存数据时，我没有采用合适的格式分隔，
 * 导致后来 ETL 非常繁琐，此处建议使用最接近原始数据的格式保存，
 * 原始数据：  参考 .file/.api/page1x100.json
 *
 * 因为 spark 可以直接处理多行 json 文本，
 * 所以这里的 jsons 默认每行存一个完整 json 对象文本。我已经踩过坑了
 */
public class SZTData {
	static String  SAVE_PATH = ORIGINAL_DATA;
	
	//TODO appKey 自己申请 https://opendata.sz.gov.cn/data/api/toApiDetails/29200_00403601
	static String appKey = "203eaa59510b482daff5ce1d8b68341f";
	
	/** 这个过程可能花费一个通宵，如果中断，查看已保存数据最后一条的 page，然后调整 i 的起始值继续抓取
	 * 使用 test 可以保存每次运行的历史日志
	 */
	public static String getData(int i) throws InterruptedException {

		String s = HttpUtil.get("https://opendata.sz.gov.cn/api/29200_00403601/1/service.xhtml?page=" + i + "&rows=1000&appKey=" + appKey);
		// 一定要加换行符，否则以后处理起来会是灾难。
		// 一定要加换行符，否则以后处理起来会是灾难。
		// 一定要加换行符，否则以后处理起来会是灾难。
		FileUtil.appendUtf8String(s + "\n", SAVE_PATH);
		System.out.println(s);
		Thread.sleep(1000);

		return s;
	}
}
