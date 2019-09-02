package org.shamit.hadoop.fs.driver.test.manual;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.shamit.hadoop.fs.driver.test.TestConfig;

import jnr.ffi.Platform;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.Flock;
import ru.serce.jnrfuse.struct.FuseBufvec;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.FusePollhandle;
import ru.serce.jnrfuse.struct.Statvfs;
import ru.serce.jnrfuse.struct.Timespec;



public class HadoopFSTest extends FuseStubFS {
	private final static Logger LOGGER =  
            Logger.getLogger("HadoopFS"); 
	
	FileSystem fs=null;
	
	public HadoopFSTest(String hdfsUrl) {
		try {
			LOGGER.info("HDFS COnnection init, URL = "+hdfsUrl);
			fs = FileSystem.get(URI.create(hdfsUrl), new Configuration());
			String rootDir = "/";
			FileStatus fStat = fs.getFileStatus(new Path(rootDir));
			LOGGER.info("HDFS Got stats on root dir = "+fStat);
			
			
		} catch (IOException e) {
			throw new RuntimeException("Could not init reference to Hadoop FileSystem",e);
		}
	}
	
	public static void main(String[] args) {
		HadoopFSTest hdfs = new HadoopFSTest(TestConfig.HDFS_ROOT_URL);
        try {
            String path;
            switch (Platform.getNativePlatform().getOS()) {
                case WINDOWS:
                    path = "J:\\";
                    break;
                default:
                    path = "/tmp/hadoop";
            }
            hdfs.mount(Paths.get(path), true, true);
        } finally {
        	hdfs.umount();
        }
	}

	int readFileAttributes(Path p,FileStat stat) {
		try {
			FileStatus fStat = fs.getFileStatus(p);
			if(fStat.isDirectory()) {
				stat.st_mode.set(FileStat.S_IFDIR | FileStat.ALL_READ);	
			}else {
				stat.st_mode.set(FileStat.S_IFREG | FileStat.ALL_READ);
			}
            stat.st_size.set(fStat.getLen());
            stat.st_uid.set(getContext().uid.get());
            stat.st_gid.set(getContext().gid.get());
            stat.st_mtim.tv_sec.set(fStat.getModificationTime()/1000);
		} catch (IllegalArgumentException | IOException e) {
			LOGGER.info(e.getMessage());
			return -1;
		}
		return 0;
	}
	
	@Override
	public int getattr(String path, FileStat stat) {
		LOGGER.info("getattr on "+path);
		return readFileAttributes(new Path(path), stat);
	}


	@Override
	public int open(String path, FuseFileInfo fi) {
		LOGGER.info("open for ::"+path);
		return super.open(path, fi);
	}

	@Override
	public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
		LOGGER.info("read for ::"+path);
		Path p = new Path(path);
		try (InputStream in = fs.open(p)) {
			byte b[]=new byte[(int)size];
			int numBytes=in.read(b, (int)offset, (int)size);
			buf.put(0, b, 0, (int)size);
			return numBytes;
		} catch (IllegalArgumentException | IOException e) {
			LOGGER.info(e.getMessage());
			return -1;
		}
	}

	@Override
	public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
		
		return super.write(path, buf, size, offset, fi);
	}

	@Override
	public int statfs(String path, Statvfs stbuf) {
		LOGGER.info("statfs for ::"+path);
		return super.statfs(path, stbuf);
	}


	@Override
	public int opendir(String path, FuseFileInfo fi) {
		LOGGER.info("HDFS OpenDir on "+path);
		return super.opendir(path, fi);
	}

	@Override
	public int readdir(String path, Pointer buf, FuseFillDir filter, long offset, FuseFileInfo fi) {
		LOGGER.info("HDFS ReadDir on "+path);
		FileStatus fStat;
		try {
			fStat = fs.getFileStatus(new Path(path));
			if(!fStat.isDirectory()) {
				return -1;
			}else {
		        filter.apply(buf, ".", null, 0);
		        filter.apply(buf, "..", null, 0);
				FileStatus[] fileStatus = fs.listStatus(new Path(path));
			    for (FileStatus fileStat : fileStatus) {
			    	String name = fileStat.getPath().getName().toString();
			    	filter.apply(buf, name, null, 0);	
			    }
			}
		} catch (IllegalArgumentException | IOException e) {
			LOGGER.log(Level.WARNING, "Can't read directory::"+path, e);
			return -1;
		}
		return 0;
	}


	@Override
	public int read_buf(String path, Pointer bufp, long size, long off, FuseFileInfo fi) {
		LOGGER.info("read_buf for ::"+path);
		return super.read_buf(path, bufp, size, off, fi);
	}


}
