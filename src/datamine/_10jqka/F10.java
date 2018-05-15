package datamine._10jqka;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.bmtech.datamine.AllStock;
import com.bmtech.datamine.Stock;
import com.bmtech.utils.Consoler;

import datamine._10jqka.CrawlConfig.URLInfo;

public class F10 {
	public String toHistoryUrl(Stock stock, int year) {

		return "http://basic.10jqka.com.cn/32/" + Stock.formatStockCode(stock.getCode()) + "/";

	}

	public void download() throws Exception {
		List<URLInfo> urls = new ArrayList<>();
		String yearNum = new SimpleDateFormat("yyyy").format(System.currentTimeMillis());
		Iterator<Stock> itr = AllStock.instance.getStocks();
		int year = Integer.parseInt(yearNum);
		while (itr.hasNext()) {
			Stock stock = itr.next();

			String url = toHistoryUrl(stock, year);
			urls.add(new URLInfo(url, stock));
		}
		System.out.println("start crawl");
		CrawlConfig conf = new CrawlConfig() {
			@Override
			public boolean callback(URLInfo info, String html) throws Exception {
				return true;

			}
		};
		conf.urls = urls;
		conf.savePath = conf.path("F10");
		int updated = 0;
		int sim = 0;
		int misMatch = 0;
		conf.crawl();
		Consoler.println("updated = %s,    sim = %s    misMatch = %s", updated, sim, misMatch);
	}

	public static void main(String[] args) throws Exception {
		F10 crl = new F10();
		crl.download();
	}
}
