package datamine._10jqka;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.bmtech.utils.ForEach;
import com.bmtech.utils.Misc;
import com.bmtech.utils.bmfs.MDir;
import com.bmtech.utils.bmfs.MFileReader;
import com.bmtech.utils.bmfs.MFileReaderIterator;

public class F10Parser {
	MDir mdir;
	File saveTo;

	public F10Parser(File mdirDir, File saveTo) throws IOException {
		mdir = MDir.open(mdirDir);
		this.saveTo = saveTo;
	}

	public void itr() throws Exception {
		MFileReaderIterator itr = mdir.openReader();
		Map<String, Map<String, String>> datas = new HashMap<>();
		while (itr.hasNext()) {
			MFileReader e = itr.next();
			String name = e.getMfile().getName();
			byte[] bs = e.getBytes();
			String html = new String(bs, "utf8");
			Map<String, String> map = parse(html);
			datas.put(name, map);
		}
		String str = Misc.toJson(datas);
		Misc.saveToDisk(saveTo, str);
	}

	public Map<String, String> parse(String html) throws Exception {
		Document doc = Jsoup.parse(html);

		Map<String, String> info = new HashMap<>();

		Elements items = doc.select("#profile .mt10 td");
		ForEach.asc(items, (e, i) -> {
			Elements spans = e.select("span");
			String name = spans.get(0).text().trim();
			String value = spans.get(1).text().trim();
			if (name.endsWith(":") || name.endsWith("：")) {
				name = name.substring(0, name.length() - 1);
			}
			info.put(name, value);
			if (spans.size() > 2) {
				String text = spans.text();
				String pm = Misc.substring(text, "同行业排名第", "位");
				// Consoler.println("\n--> " + spans.text());
				// Consoler.println("\n---> " + pm);
				if (pm != null && pm.length() > 0) {
					info.put(name + "-排名", pm);
				}
			}
		});

		// info.fenlei = text(doc, '#profile .mt10 td :contains(分类：)')
		// info.shiyinglvdongtai = text(doc, '#profile .mt10 td
		// :contains(市盈率\(动态\)：)')
		// info.shiyinglvjingtai = text(doc, '#profile .mt10 td
		// :contains(市盈率\(静态\)：)')
		// info.meigushouyi = text(doc, '#profile .mt10 td :contains(每股收益：)')
		// info.zuixinjiejin = text(doc, '#profile .mt10 td :contains(最新解禁：)')

		// printJson(info);
		return info;
	}

	public static void main(String[] args) throws Exception {
		File f = new File("mdir/CrawlerTmpHome/F10/2016-12-24");
		F10Parser p = new F10Parser(f, new File("./config/stockBasic.json"));
		p.itr();
	}
}
