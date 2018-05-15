package datamine._10jqka;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import com.bmtech.datamine.Stock;
import com.bmtech.utils.Consoler;
import com.bmtech.utils.Misc;
import com.bmtech.utils.bmfs.MDir;
import com.bmtech.utils.bmfs.MFile;
import com.bmtech.utils.http.HttpCrawler;
import com.bmtech.utils.log.LogHelper;

public class CrawlConfig {
	LogHelper log = new LogHelper("crawlConfig");
	/** default 0 **/
	int sleepWhenCrawlSuccess = 0;
	/** default 0 **/
	int sleepWhenCrawlError = 0;
	/** default false **/
	boolean stopWhenCrawlError = false;
	/** usually config as: conf.savePath = conf.path(saveName) **/
	String savePath = "";
	/** the urls to crawl, data type as: [{'url':urlStr}, ...] **/
	List<URLInfo> urls = new ArrayList<>();
	Exception err;
	private List<String> warns = new LinkedList<String>();

	/**
	 * the parse function, if not set use default interactive function <br>
	 * PLEASE set this function if need parse, if not need parse, please set it
	 * as function(){return true} <br>
	 * 
	 * @param urlInfo
	 *            {object} a object has field 'url'...
	 * @param html
	 *            {string} the html got from urlInfo.url
	 * @return {boolean} if false return the crawl loop will stop
	 * @throws Exception
	 * @throws if
	 *             throw exception, the crawl loop will stop, so please be sure
	 *             the exception be catched
	 */
	public boolean callback(URLInfo urlInfo, String html) throws Exception {
		print(html);
		print("crawl url ok, in callback now");
		return Consoler.confirm("continue?");
	}

	public String path(String typeName) {
		return "mdir/CrawlerTmpHome/" + Misc.formatFileName(typeName) + "/"
				+ new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis());
	}

	/**
	 * the crawl caculate info
	 */
	class CrawlRoundInfo {
		int checkCount = 0;
		int okCrawl = 0;
		int crawlCount = 0;
		int failCount = 0;
		int parseFail = 0;

		public String toString() {
			return Misc.toString(this);
		}
	}

	// CrawlRoundInfo crawlRoundInfo = new CrawlRoundInfo();

	/**
	 * the report function reporting the excute result
	 * 
	 * @return
	 */
	public boolean report(CrawlRoundInfo crawlRoundInfo) {
		print("crawlReport:");
		print(crawlRoundInfo);
		boolean mayError = false;
		if (crawlRoundInfo.failCount > 0 || crawlRoundInfo.parseFail > 0
				|| this.urls.size() != crawlRoundInfo.checkCount) {
			print("Error!!! maybe not all success");
			mayError = true;
		}
		if (null != this.err) {
			print("errors:");
			this.err.printStackTrace();
		}
		if (this.warns.size() > 0) {
			print("warning:");
			this.warns.forEach(new Consumer<String>() {

				@Override
				public void accept(String t) {
					print("[warn] " + t);

				}
			});
		}
		return mayError;
	};

	/**
	 * the function start crawling. <br>
	 *
	 * you can see report use report ( ) , or just view the member
	 * crawlRoundInfo
	 * 
	 * @throws Exception
	 *
	 */
	public void crawl() throws Exception {
		if (this.urls.size() == 0 || this.urls.get(0).url == null) {
			print("urls not sets, or has not 'url' field in urls items");
			return;
		}

		if (this.savePath.length() == 0) {
			this.savePath = "mdir/CrawlerTmpHome/" + Misc.formatFileName(urls.get(0).url) + "/"
					+ new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis());
			File mdirDir = new File(this.savePath);
			if (mdirDir.exists()) {
				print("savePath not sets, and create tmp file fail : " + mdirDir);
				return;
			}
		}

		print("saving to " + this.savePath);
		MDir mdir = MDir.open4Write(new File(this.savePath));
		CrawlRoundInfo crawlRoundInfo;
		int tryNum = 3;
		while (tryNum > 0) {
			System.out.println("tryNum " + tryNum);
			;
			try {
				crawlRoundInfo = crawlUrls(this.urls, mdir, this);
			} finally {
				mdir.close();
			}
			boolean mayError = this.report(crawlRoundInfo);
			if (!mayError)
				break;
			tryNum--;
		}
	}

	public void addWar(String warn) {
		this.warns.add(warn);
		// sprint("crawlConfig init")
	}

	static void print(Object xt) {
		System.out.println(xt);
	}

	public static class URLInfo {
		String name;
		String html;
		boolean isCrawl;
		String url;
		Stock stock;

		public URLInfo(String url, Stock stock) {
			this(url, stock, stock.getCode());
		}

		public URLInfo(String url, Stock stock, String name) {
			this.name = name;
			this.url = url;
			this.stock = stock;
		}
	}

	public CrawlRoundInfo crawlUrls(List<URLInfo> urls, MDir mdir, CrawlConfig conf) throws Exception {
		return crawlIterator(urls, mdir, conf);
	}

	/**
	 * @private
	 *
	 * @param urls
	 * @param mdir
	 * @param conf
	 *            {CrawlConfig}
	 * @return
	 * @return
	 * @throws Exception
	 */
	public CrawlRoundInfo crawlIterator(List<URLInfo> urls, MDir mdir, CrawlConfig conf) throws Exception {
		if (conf == null) {
			throw new Exception("conf not set");
		}

		// print(callback)
		CrawlRoundInfo crawlRoundInfo = new CrawlRoundInfo();
		try {
			long st = System.currentTimeMillis();
			for (URLInfo urlInfo : urls) {
				// if (Misc.randInt(0, 10) % 5 != 0)
				// continue;
				boolean shouldBreak = false;
				crawlRoundInfo.checkCount++;
				if (urlInfo.isCrawl) {
					continue;
				}
				crawlRoundInfo.crawlCount++;
				try {
					urlInfo.html = crawlWithMdir(urlInfo.url, urlInfo.name, mdir);
					urlInfo.isCrawl = true;
					print("got " + urlInfo.html.length() / 1000.0 + " KB for " + urlInfo.url);
					crawlRoundInfo.okCrawl++;

					if (conf.sleepWhenCrawlSuccess > 0) {
						Misc.sleep(conf.sleepWhenCrawlSuccess);
					}
				} catch (Exception e) {
					crawlRoundInfo.failCount++;
					print(e);
					if (conf.stopWhenCrawlError) {
						break;
					}
					if (conf.sleepWhenCrawlError > 0) {
						Misc.sleep(conf.sleepWhenCrawlError);
					}
				} finally {
					if (crawlRoundInfo.checkCount % 10 == 0) {
						Consoler.println("!!!!!total %s, crawled %s use %s seconds, average %s page per second",
								urls.size(), crawlRoundInfo.checkCount, (System.currentTimeMillis() - st) / 1000,
								Math.floor(crawlRoundInfo.checkCount * 1000 * 10 / (System.currentTimeMillis() - st))
										/ 10);
					}
					try {
						if (!conf.callback(urlInfo, urlInfo.html)) {
							crawlRoundInfo.parseFail++;
							shouldBreak = true;
						}
					} catch (Exception exc) {
						crawlRoundInfo.parseFail++;
						conf.err = exc;
						print(exc);

						// if(!confirm("continue?")){
						// break
						// }
					}
				}
				if (shouldBreak) {
					break;
				}
			}

			print("crawl info " + Misc.toString(crawlRoundInfo));

		} finally {
			mdir.fsyn();
		}
		return crawlRoundInfo;
	}

	String crawlWithMdir(String urlStr, String name, MDir mdir) throws Exception {
		if (null == mdir) {
			throw new Exception("mdir not set");
		}
		MFile mfile = mdir.getMFileByName(name);
		String ret;
		print("crawling " + name + " with url " + urlStr);
		if (mfile != null) {
			log.debug("hit in mdir for url %s", urlStr);
			byte[] bytes = mfile.getBytes();
			return new java.lang.String(bytes, "utf-8");
		} else {
			URL url = new URL(urlStr);
			HttpCrawler crl = HttpCrawler.makeCrawler(url);
			ret = crl.getString();
			InputStream ips = new ByteArrayInputStream(ret.getBytes("utf-8"));
			mdir.addFile(name, ips);
		}
		return ret;
	}
}
