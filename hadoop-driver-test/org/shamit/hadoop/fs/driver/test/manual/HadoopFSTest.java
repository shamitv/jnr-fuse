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
	public int readlink(String path, Pointer buf, long size) {
		
		return super.readlink(path, buf, size);
	}

	@Override
	public int mknod(String path, long mode, long rdev) {
		
		return super.mknod(path, mode, rdev);
	}

	@Override
	public int mkdir(String path, long mode) {
		
		return super.mkdir(path, mode);
	}

	@Override
	public int unlink(String path) {
		
		return super.unlink(path);
	}

	@Override
	public int rmdir(String path) {
		
		return super.rmdir(path);
	}

	@Override
	public int symlink(String oldpath, String newpath) {
		
		return super.symlink(oldpath, newpath);
	}

	@Override
	public int rename(String oldpath, String newpath) {
		
		return super.rename(oldpath, newpath);
	}

	@Override
	public int link(String oldpath, String newpath) {
		
		return super.link(oldpath, newpath);
	}

	@Override
	public int chmod(String path, long mode) {
		
		return super.chmod(path, mode);
	}

	@Override
	public int chown(String path, long uid, long gid) {
		
		return super.chown(path, uid, gid);
	}

	@Override
	public int truncate(String path, long size) {
		
		return super.truncate(path, size);
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
	public int flush(String path, FuseFileInfo fi) {
		
		return super.flush(path, fi);
	}

	@Override
	public int release(String path, FuseFileInfo fi) {
		
		return super.release(path, fi);
	}

	@Override
	public int fsync(String path, int isdatasync, FuseFileInfo fi) {
		
		return super.fsync(path, isdatasync, fi);
	}

	@Override
	public int setxattr(String path, String name, Pointer value, long size, int flags) {
		
		return super.setxattr(path, name, value, size, flags);
	}

	@Override
	public int getxattr(String path, String name, Pointer value, long size) {
		
		return super.getxattr(path, name, value, size);
	}

	@Override
	public int listxattr(String path, Pointer list, long size) {
		
		return super.listxattr(path, list, size);
	}

	@Override
	public int removexattr(String path, String name) {
		
		return super.removexattr(path, name);
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
	public int releasedir(String path, FuseFileInfo fi) {
		
		return super.releasedir(path, fi);
	}

	@Override
	public int fsyncdir(String path, FuseFileInfo fi) {
		
		return super.fsyncdir(path, fi);
	}

	@Override
	public Pointer init(Pointer conn) {
		
		return super.init(conn);
	}

	@Override
	public void destroy(Pointer initResult) {
		
		super.destroy(initResult);
	}

	@Override
	public int access(String path, int mask) {
		
		return super.access(path, mask);
	}

	@Override
	public int create(String path, long mode, FuseFileInfo fi) {
		
		return super.create(path, mode, fi);
	}

	@Override
	public int ftruncate(String path, long size, FuseFileInfo fi) {
		
		return super.ftruncate(path, size, fi);
	}

	@Override
	public int fgetattr(String path, FileStat stbuf, FuseFileInfo fi) {
		LOGGER.info("fgetattr for ::"+path);
		return readFileAttributes(new Path(path), stbuf);
	}

	@Override
	public int lock(String path, FuseFileInfo fi, int cmd, Flock flock) {
		
		return super.lock(path, fi, cmd, flock);
	}

	@Override
	public int utimens(String path, Timespec[] timespec) {
		
		return super.utimens(path, timespec);
	}

	@Override
	public int bmap(String path, long blocksize, long idx) {
		
		return super.bmap(path, blocksize, idx);
	}

	@Override
	public int ioctl(String path, int cmd, Pointer arg, FuseFileInfo fi, long flags, Pointer data) {
		
		return super.ioctl(path, cmd, arg, fi, flags, data);
	}

	@Override
	public int poll(String path, FuseFileInfo fi, FusePollhandle ph, Pointer reventsp) {
		
		return super.poll(path, fi, ph, reventsp);
	}

	@Override
	public int write_buf(String path, FuseBufvec buf, long off, FuseFileInfo fi) {
		
		return super.write_buf(path, buf, off, fi);
	}

	@Override
	public int read_buf(String path, Pointer bufp, long size, long off, FuseFileInfo fi) {
		LOGGER.info("read_buf for ::"+path);
		return super.read_buf(path, bufp, size, off, fi);
	}

	@Override
	public int flock(String path, FuseFileInfo fi, int op) {
		
		return super.flock(path, fi, op);
	}

	@Override
	public int fallocate(String path, int mode, long off, long length, FuseFileInfo fi) {
		
		return super.fallocate(path, mode, off, length, fi);
	}

}
