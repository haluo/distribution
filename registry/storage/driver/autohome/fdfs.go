package auto

import (
	"io"
	"net/http"
	"fmt"
	"io/ioutil"
	"encoding/base64"
	"encoding/json"
	"errors"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"time"
)

const DK_FDFS_URL string = "http://dk.rg.autohome.com.cn"

/**
fastdfs 操作相关
 */

type Fdfshandle struct {
	path string
	offset int64
}

func  NewFdfs(path,offset string) *Fdfshandle {
	return &Fdfshandle{path:path,offset:offset}
}

//实现 io.Reader接口
func (h *Fdfshandle) Read(p []byte) (n int, err error){
	//调用http接口
	url := DK_FDFS_URL+"/rstream?path="+h.path+"&offset="+h.offset+"&n="+len(p)
	resp, err :=http.Get(url)
	if(err != nil){
		fmt.Println("Hdfshandle Read http get err")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Hdfshandle Read ioutil  err")
	}
	fmt.Println(string(body))
	uDec, _ :=base64.StdEncoding.DecodeString(string(body))
	p = uDec;
	return len(p),nil
}

//实现 io.Closer接口
func (h *Fdfshandle) Close() error{
	return
}


//将io.Reader 写入远程fastdfs
func (h *Fdfshandle)Write(reader io.Reader) (nn int64, err error){
	//将 reder内容写入 远程 fastdfs
	buf := make([]byte, 32*1024);
	for {

		//每次读取nr条数据 写入远程
		nr, er := reader.Read(buf)
		if nr > 0 {
			//nw, ew := dst.Write(buf[0:nr])
			bstr := base64.StdEncoding.EncodeToString(buf[0:nr]);
			url := DK_FDFS_URL+"/wstream?path="+h.path+"&offset="+h.offset+"&bstr="+bstr
			resp, eg :=http.Get(url)
			if eg != nil {
				err = eg
				break
			}
			defer resp.Body.Close()
			body, ea := ioutil.ReadAll(resp.Body)
			if ea != nil {
				err = ea
				break
			}
			nw := int(body);

			if nw > 0 {
				nn += int64(nw)
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er == io.EOF {
			break
		}
		if er != nil {
			err = er
			break
		}
	}
	return
}


func (h *Fdfshandle) Stat()(info storagedriver.FileInfo,err error){
	url := DK_FDFS_URL+"/finfo?path="+h.path
	resp, eg :=http.Get(url)
	if eg != nil {
		err = eg
		return
	}
	defer resp.Body.Close()
	body, ea := ioutil.ReadAll(resp.Body)
	if ea != nil {
		err = ea
		return
	}
	var fs  FdfsInfo
	rs := string(body);
	json.Unmarshal([]byte(rs),&fs);
	return fs,nil
}

func (h *Fdfshandle) List()([]string,err error){
	url := DK_FDFS_URL+"/list?path="+h.path
	resp, eg :=http.Get(url)
	if eg != nil {
		err = eg
		return
	}
	defer resp.Body.Close()
	body, ea := ioutil.ReadAll(resp.Body)
	if ea != nil {
		err = ea
		return
	}
	var ss  []string;
	rs := string(body);
	json.Unmarshal([]byte(rs),&ss);
	return ss,nil
}

func (h *Fdfshandle) Move(topath string)(err error){
	url := DK_FDFS_URL+"/move?srcpath="+h.path+"&topath="+topath
	resp, eg :=http.Get(url)
	if eg != nil {
		err = eg
		return
	}
	defer resp.Body.Close()
	body, ea := ioutil.ReadAll(resp.Body)
	if ea != nil {
		err = ea
		return
	}
	rs := int(body);
	if rs == 0 {
		return nil
	}else {
		return errors.New("move fail ~~");
	}
}

func (h *Fdfshandle) Delete()(err error){
	url := DK_FDFS_URL+"/delete?path="+h.path
	resp, eg :=http.Get(url)
	if eg != nil {
		err = eg
		return
	}
	defer resp.Body.Close()
	body, ea := ioutil.ReadAll(resp.Body)
	if ea != nil {
		err = ea
		return
	}
	rs := int(body);
	if rs == 0 {
		return nil
	}else {
		return errors.New("delete fail ~~");
	}
}



type FdfsInfo struct {
	Path string `json:"path"`
	Size int64 `json:"size"`
	ModTime time.Time `json:"time"`
	IsDir bool `json:"isDir"`
}

func (f FdfsInfo) Path() string {
	return f.Path
}
func (f FdfsInfo) Size() int64 {
	return f.Size
}
func (f FdfsInfo) ModTime() time.Time {
	return f.ModTime
}
func (f FdfsInfo) IsDir() bool {
	return f.IsDir
}

