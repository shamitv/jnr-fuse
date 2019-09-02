package org.shamit.hadoop.fs.driver.test.manual;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.shamit.hadoop.fs.driver.test.TestConfig;

public class ListDir {
	/**
	 * @param filePath
	 * @param fs
	 * @return list of absolute file path present in given path
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static List<String> traverseDirectory(Path dirPath, FileSystem fs) throws FileNotFoundException, IOException {
	    List<String> fileList = new ArrayList<>();
	    FileStatus[] fileStatus = fs.listStatus(dirPath);
	    for (FileStatus fileStat : fileStatus) {
	        if (fileStat.isDirectory()) {
	            fileList.addAll(traverseDirectory(fileStat.getPath(), fs));
	        } else {
	            fileList.add(fileStat.getPath().toString());
	        }
	    }
	    return fileList;
	}

	public static void main(String[] args) {
		String rootDir="/";
		FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(TestConfig.HDFS_ROOT_URL), new Configuration());
			Path dirPath=new Path(rootDir);
			List<String> dirTree=traverseDirectory(dirPath, fs);
			System.out.println(dirTree);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
