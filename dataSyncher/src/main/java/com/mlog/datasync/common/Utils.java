package com.mlog.datasync.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;


public class Utils {
	private static Logger logger = LoggerFactory.getLogger(Utils.class);
	
	private static String configPath = null;
	private static Properties config = null;
	
	private static Properties dataBaseProperies = null;

	/**
	 * Convert JSON format string to token object.
	 * @param <T>
	 * @param jsonStr
	 * @return
	 */
	public static <T> Object jsonStrToObject(String jsonStr, Class<T> classType) {
		Object jsonObj = null;
		if (jsonStr == null) {
			return jsonObj;
		}

		try {
			Gson gson = new GsonBuilder().create();
			jsonObj = gson.fromJson(jsonStr, classType);
		} catch (Exception e) {
			logger.error("", e);
		}

		return jsonObj;
	}
	
	/**
	 * Convert JSON format string to Map.
	 * @param string
	 * @return
	 */
	public static Map<String, Object> jsonStrToObject(String jsonStr) {
		Map<String, Object> jsonMap = new HashMap<String, Object>();
		if (jsonStr == null) {
			return null;
		}

		try {
			ObjectMapper mapper = new ObjectMapper();
			TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
			};

			jsonMap = mapper.readValue(jsonStr, typeRef);
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}

		return jsonMap;
	}

	/**
	 * Convert JSON format string to Map.
	 * @param string
	 * @return
	 */
	public static List<HashMap<String, Object>> jsonStrToList(String jsonStr) {
		List<HashMap<String, Object>> jsonList = new ArrayList<HashMap<String, Object>>();
		if (jsonStr == null) {
			return null;
		}

		try {
			ObjectMapper mapper = new ObjectMapper();
			TypeReference<List<HashMap<String, Object>>> typeRef = new TypeReference<List<HashMap<String, Object>>>() {
			};

			jsonList = mapper.readValue(jsonStr, typeRef);
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}

		return jsonList;
	}

	/**
	 * Convert object to JSON format string.
	 * @param object
	 * @return
	 */
	public static String objectToJsonStr(Object json) {
		String jsonStr = null;
		if (json == null) {
			return null;
		}

		try {
			Gson gson = new GsonBuilder().create();
			jsonStr = gson.toJson(json);
		} catch (Exception e) {
			logger.error("", e);
		}

		return jsonStr;
	}

	public static Properties getProperties() {
		if (config == null) {
			config = new Properties();
		} else {
			return config;
		}

		try {
			if (configPath == null) {
				configPath = "./conf/server.properties";
			}
			
			FileInputStream fis = new FileInputStream(configPath);
			config.load(fis);

			fis.close();
		} catch (Exception e) {
			logger.error("", e);
			
			return null;
		}

		return config;
	}
	
	public static void setConfigPath(String configPath) {
		Utils.configPath = configPath;
	}

	public static String getProperty(String key) {
		if (config == null) {
			config = getProperties();
		}

		if (config == null) {
			logger.error("Could not load propeties.");
			return null;
		}

		String value = config.getProperty(key);
		if (value == null || value.length() == 0) {
			logger.error("failure get property [{}]=[{}]", key, value);
			return null;
		} else {
			logger.debug("success get property [{}]=[{}]", key, value);
		}

		return value;
	}
	
	public static int loadProperties(String path) {
		configPath = path;
		return 0;
	}

    public static int loadLogConfigs(String path) {
    	LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();

        try {
            configurator.doConfigure(path);
        } catch (JoranException e) {
            e.printStackTrace();
        }
        
        return 0;
    }

	/**
	 * 파일의 내용을 스트링으로 반환.
	 * @param path
	 * @return
	 */
	public static String readFile(String path) {
		String jsonStr = "";

		File file = new File(path);
		
		if (file.exists() == false) {
			logger.error("Could not find file in {}", path);
			return null;
		}

		try {
			String temp;

			BufferedReader br = new BufferedReader(new FileReader(file));
			while ((temp = br.readLine()) != null) {
				jsonStr += temp;
			}

			br.close();
		} catch (Exception e) {
			logger.error("", e);
		}

		return jsonStr;
	}

	/**
	 * 데이터베이스 프로퍼티 반환.
	 * @return
	 */
	public synchronized static Properties getDataBaseProperties() {
		if (dataBaseProperies != null) {
			return dataBaseProperies;
		} else {
			dataBaseProperies = new Properties();
		}
		
		String jsonInfo = readFile(getProperty(Constant.DATABASE_INFO_PATH));
		if (jsonInfo == null) {
			return null;
		}

		HashMap<String, Object> store = (HashMap<String, Object>) jsonStrToObject(jsonInfo);
		if (store == null) {
			return null;
		}

		for (String key : store.keySet()) {
			String val = store.get(key).toString();
			dataBaseProperies.setProperty(key, val);
		}

		return dataBaseProperies;
	}

	/**
	 * 데이터베이스 프로퍼티 반환.
	 * @return
	 */
	public synchronized static Properties getDataBaseProperties(String dataSource) {
		Properties dataBaseProperies = new Properties();

		String jsonInfo = readFile(getProperty(Constant.DATABASE_INFO_PATH));
		if (jsonInfo == null) {
			return null;
		}

		List<HashMap<String, Object>> list = jsonStrToList(jsonInfo);
		
		for (HashMap<String, Object> map : list) {
			if (dataSource.equals(map.get("datasource"))) {
				for (String key : map.keySet()) {
					String val = map.get(key).toString();
					dataBaseProperies.setProperty(key, val);
				}
			}			
		}

		return dataBaseProperies;
	}


	public static String convDateFormat(String time, String sourceFormat, String targetFormat) {
		String convDateFormat = "";

		try {
			if (time != null) {
				time = time.substring(0, time.length() - 1) + "0";
			}

			DateFormat sourceDateFormat = new SimpleDateFormat(sourceFormat);
			Date date = sourceDateFormat.parse(time);

			DateFormat targetDateFormat = new SimpleDateFormat(targetFormat);
			convDateFormat = targetDateFormat.format(date);
		} catch (Exception e) {
			logger.error("", e);
		}

		return convDateFormat;
	}

	public static List<String> getFileList(String path) {
		List<String> list = null;

		try (Stream<Path> walk = Files.walk(Paths.get(path))) {
			list = walk.filter(Files::isRegularFile).map(x -> x.toString()).collect(Collectors.toList());
		} catch (IOException e) {
			e.printStackTrace();
		}

		return list;
	}

	public static Map<String, Object> parseRule(String data_rule_path, String tagName) {
        File inputFile = new File(data_rule_path);
        
        Map<String, Object> rule = new HashMap<String, Object>();

        try {
        	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        	DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        	Document doc = dBuilder.parse(inputFile);
        	doc.getDocumentElement().normalize();

        	NodeList nList = doc.getElementsByTagName(tagName);
        	
        	Node n = nList.item(0);
        	Element eElement = (Element) n;			 
        	
        	String x = eElement.getElementsByTagName("sql").item(0).getTextContent();
        	String y = eElement.getElementsByTagName("datasource").item(0).getTextContent();
			rule.put(CommonStr.DATASOURCE, y);

			rule.put(CommonStr.SQL, x);
		} catch (Exception e) {
			logger.error("", e);
		}
		
		return rule;
	}

}
