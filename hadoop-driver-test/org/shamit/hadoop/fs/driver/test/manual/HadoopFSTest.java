package org.shamit.hadoop.fs.driver.test.manual;

import java.nio.file.Paths;

import jnr.ffi.Platform;
import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.examples.MemoryFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.Flock;
import ru.serce.jnrfuse.struct.FuseBufvec;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.FusePollhandle;
import ru.serce.jnrfuse.struct.Statvfs;
import ru.serce.jnrfuse.struct.Timespec;

public class HadoopFSTest extends FuseStubFS {

	public static void main(String[] args) {
		HadoopFSTest memfs = new HadoopFSTest();
        try {
            String path;
            switch (Platform.getNativePlatform().getOS()) {
                case WINDOWS:
                    path = "J:\\";
                    break;
                default:
                    path = "/tmp/hadoop";
            }
            memfs.mount(Paths.get(path), true, true);
        } finally {
            memfs.umount();
        }
	}

	@Override
	public int getattr(String path, FileStat stat) {
		
		return super.getattr(path, stat);
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
		
		return super.open(path, fi);
	}

	@Override
	public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
		
		return super.read(path, buf, size, offset, fi);
	}

	@Override
	public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
		
		return super.write(path, buf, size, offset, fi);
	}

	@Override
	public int statfs(String path, Statvfs stbuf) {
		
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
		
		return super.opendir(path, fi);
	}

	@Override
	public int readdir(String path, Pointer buf, FuseFillDir filter, long offset, FuseFileInfo fi) {
		
		return super.readdir(path, buf, filter, offset, fi);
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
		
		return super.fgetattr(path, stbuf, fi);
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
