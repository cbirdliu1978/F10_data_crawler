package datamine._10jqka;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.bmtech.datamine.AllStock;
import com.bmtech.datamine.Stock;
import com.bmtech.datamine.data.mday.KLinePoint;
import com.bmtech.utils.Charsets;
import com.bmtech.utils.Consoler;
import com.bmtech.utils.Misc;
import com.bmtech.utils.io.FileGet;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import datamine._10jqka.CrawlConfig.URLInfo;

public class HistroyDayCrawl {

	static final File incDir = new File("/ext/inc-day-ths/");
	static {
		incDir.mkdirs();
	}

	public File yearFile(Stock stk, int yearNum) {
		File f = new File(incDir, yearNum + "/" + stk.getCode());
		if (!f.getParentFile().exists()) {
			f.getParentFile().mkdirs();
		}
		return f;
	}

	public void saveYearData(Stock stk, int yearNum, List<KLinePoint> yearList, boolean forceSave) throws IOException {
		String txt = Misc.toJson(yearList);
		File f = yearFile(stk, yearNum);
		boolean save = true;
		if (f.exists()) {
			if (!forceSave) {
				boolean needSave = Consoler.confirm("override file " + f.getAbsolutePath() + "? ");
				if (!needSave) {
					save = false;
				}
			}
		}
		if (save)
			Misc.saveToDisk(f, txt);

	}

	public List<KLinePoint> loadYear(Stock stk, int year) throws IOException {
		List<KLinePoint> ret = loadYearInner(stk, year);
		return ret;
	}

	public List<KLinePoint> loadYearInner(Stock stk, int year) throws IOException {
		File yearFile = yearFile(stk, year);
		if (yearFile.exists()) {
			String txt = FileGet.getStr(yearFile, Charsets.UTF8_CS);
			ObjectMapper m = new ObjectMapper();
			JavaType jt = m.getTypeFactory().constructParametrizedType(List.class, ArrayList.class, KLinePoint.class);
			List<KLinePoint> lst = m.readValue(txt, jt);
			return lst;
		} else {
			return Collections.emptyList();
		}
	}

	public String toHistoryUrl(Stock stock, int year) {
		String prf;
		if (stock.getCode().startsWith("6")) {
			prf = "sh_";
		} else {
			prf = "sz_";
		}
		return "http://d.10jqka.com.cn/v2/line/" + prf + Stock.formatStockCode(stock.getCode()) + "/01/" + year + ".js";

	}

	public List<KLinePoint> parseHistoryData(String html, String code) {

		String txt = Misc.substring(html, "\"data\":\"", "\"");// js.substring(pos
																// +1,js.length
																// -1)//js.replaceAll(".+_last",
																// "eval")
		List<KLinePoint> list = new ArrayList<>();
		if (txt != null) {
			String[] days = txt.split(";");
			// int cnt = 0;

			for (String day : days) {
				// cnt++;
				String[] tokens = day.split(",");
				boolean errorFmt = false;
				for (String x : tokens) {
					if (x.length() == 0) {
						errorFmt = true;
					}
				}
				if (!errorFmt) {
					KLinePoint oneDay = new KLinePoint(tokens);
					oneDay.setCode(code);
					list.add(oneDay);
				}
			}
			return list;
		} else {
			return list;
		}
	}

	public void downloadThisYear() throws Exception {
		List<URLInfo> urls = new ArrayList<>();
		String yearNum = "2016";// new
								// SimpleDateFormat("yyyy").format(System.currentTimeMillis());
		Iterator<Stock> itr = AllStock.instance.getStocks();
		int year = Integer.parseInt(yearNum);
		while (itr.hasNext()) {
			Stock stock = itr.next();

			String url = toHistoryUrl(stock, year);
			urls.add(new URLInfo(url, stock));
		}
		downloadYear(year, urls);
	}

	public void downloadYear(int yearNum, List<URLInfo> urls) throws Exception {
		System.out.println("start crawl");
		CrawlConfig conf = new CrawlConfig() {
			@Override
			public boolean callback(URLInfo info, String html) throws Exception {
				try {
					List<KLinePoint> yearList = parseHistoryData(info.html, info.stock.getCode());
					List<KLinePoint> oldYear = loadYear(info.stock, yearNum);
					if (yearList.size() == oldYear.size()) {
						// match!
						log.debug("year size match, skip for %s", info.stock);
					} else if (yearList.size() < oldYear.size()) {
						// skip
						log.debug("year size too small, skip for %s", info.stock);

					} else {
						saveYearData(info.stock, yearNum, yearList, true);
					}

				} catch (Exception e) {
					e.printStackTrace();
					throw e;
				}

				return true;

			}
		};
		conf.urls = urls;
		conf.savePath = conf.path("history.this.year." + yearNum);
		int updated = 0;
		int sim = 0;
		int misMatch = 0;
		conf.sleepWhenCrawlSuccess = 1000;
		conf.crawl();
		Consoler.println("updated = %s,    sim = %s    misMatch = %s", updated, sim, misMatch);
	}

	public static void main(String[] args) throws Exception {
		HistroyDayCrawl crl = new HistroyDayCrawl();
		crl.downloadThisYear();
	}
}
