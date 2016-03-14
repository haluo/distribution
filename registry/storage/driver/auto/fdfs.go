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
	"strconv"
	"strings"
	"net/url"
)

const DK_FDFS_URL string = "http://dk.rg.autohome.com.cn"

/**
fastdfs 操作相关
 */

type Fdfshandle struct {
	path string
	offset int64
}

func  NewFdfs(path string,offset int64) *Fdfshandle {
	return &Fdfshandle{path:path,offset:offset}
}

//实现 io.Reader接口
func (h *Fdfshandle) Read(p []byte) (n int, err error){
	//调用http接口
	dlen,elen := h.getlength();
	if elen!= nil {
		return 0,elen
	}
	if dlen==int(h.offset){
		return 0,io.EOF
	}
	isEnd := false;
	fdfsn:=len(p);
	if len(p) >= dlen-int(h.offset){
		//p = p[h.offset:dlen]
		fdfsn = dlen-int(h.offset)
		isEnd = true;
	}
	url := DK_FDFS_URL+"/rstream?path="+h.path+"&offset="+strconv.FormatInt(h.offset,10)+"&n="+strconv.Itoa(fdfsn)
	//fmt.Println(url);
	resp, err :=http.Get(url)
	if(err != nil){
		fmt.Println("Hdfshandle Read http get err")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Hdfshandle Read ioutil  err")
	}
	if body==nil{
		return 0,errors.New("NOT FUND");
	}
	str := string(body);
	res := strings.Split(str,"|");

	code,_:= strconv.Atoi(res[0]);
	resut := res[1];
	//fmt.Println("rstream--path="+h.path+"&offset="+strconv.FormatInt(h.offset,10)+"&n="+strconv.Itoa(len(p))+"-->"+string(body))
	if code==0 {
		uDec, _ :=base64.URLEncoding.DecodeString(resut)
		//p = append(uDec,p[len(uDec):]...);
		//n = len(uDec)
		n = copy(p,uDec)
		if(isEnd){
			//fmt.Println("读取结束=="+string(p)+"--n="+strconv.Itoa(n))
			return n,io.EOF
		}else {
			h.offset+=int64(n)
			//fmt.Println("读取没结束==")
			return n, nil
		}
	}else if code ==1{
		return 0, storagedriver.PathNotFoundError{Path: h.path}
	}else if code == 2{
		return 0,storagedriver.InvalidOffsetError{Path: h.path, Offset: h.offset}
	}else {
		return 0,errors.New(resut)
	}

	//if str=="" || len(str)==0{
	//	return 0, storagedriver.PathNotFoundError{Path: h.path}
	//}
	//uDec, _ :=base64.StdEncoding.DecodeString(string(body))
	//p = uDec;
	//return len(p),nil
}

//实现 io.Closer接口
func (h *Fdfshandle) Checkoffset() (bool,error){
	url := DK_FDFS_URL+"/getlength?path="+h.path
	resp, err :=http.Get(url)
	if(err != nil){
		fmt.Println("Hdfshandle Checkoffset http get err")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Hdfshandle Read ioutil  err")
	}
	if body==nil{
		return false,errors.New("NOT FUND");
	}
	str := string(body);
	res := strings.Split(str,"|");

	code,_:= strconv.Atoi(res[0]);
	resut := res[1];
	if code==0 {
		length,_:= strconv.Atoi(resut)
		if int64(length) >= h.offset{
			return true,nil
		}else {
			return false,storagedriver.InvalidOffsetError{Path: h.path, Offset: h.offset}
		}
	}else if code ==1{
		return false, storagedriver.PathNotFoundError{Path: h.path}
	}else {
		return false,errors.New(resut)
	}
}



//实现 io.Closer接口
func (h *Fdfshandle) getlength() (int,error){
	url := DK_FDFS_URL+"/getlength?path="+h.path
	resp, err :=http.Get(url)
	if(err != nil){
		fmt.Println("Hdfshandle Checkoffset http get err")
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Hdfshandle Read ioutil  err")
	}
	if body==nil{
		return 0,errors.New("NOT FUND");
	}
	str := string(body);
	res := strings.Split(str,"|");

	code,_:= strconv.Atoi(res[0]);
	resut := res[1];
	if code==0 {
		length,_:= strconv.Atoi(resut)
		return length,nil
	}else if code ==1{
		return 0, storagedriver.PathNotFoundError{Path: h.path}
	}else {
		return 0,errors.New(resut)
	}
}


//实现 io.Closer接口
func (h *Fdfshandle) Close() error{
	return nil
}


//将io.Reader 写入远程fastdfs
func (h *Fdfshandle)Write(reader io.Reader) (nn int64, err error){
	//将 reder内容写入 远程 fastdfs
	buf := make([]byte, 60*1024);
	offset := h.offset;
	for {

		//每次读取nr条数据 写入远程
		nr, er := reader.Read(buf)
		if nr > 0 {
			bstr := base64.URLEncoding.EncodeToString(buf[0:nr]);
			//url := DK_FDFS_URL+"/wstream?path="+h.path+"&offset="+strconv.FormatInt(offset,10)+"&bstr="+bstr
			//fmt.Println(url);
			//resp, eg :=http.Get(url)

			v := url.Values{}
			v.Set("path", h.path)
			v.Set("offset", strconv.FormatInt(offset,10))
			v.Set("bstr", bstr)
			fbody := ioutil.NopCloser(strings.NewReader(v.Encode())) //把form数据编下码
			client := &http.Client{}
			req, _ := http.NewRequest("POST", DK_FDFS_URL+"/wstream", fbody)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
			resp, eg := client.Do(req) //发送
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

			nw,_:= strconv.Atoi(string(body));
			//fmt.Println(h.path+"======================>nw="+strconv.Itoa(nw)+"   nr="+strconv.Itoa(nr));
			if nw > 0 {
				nn += int64(nw)
				offset+=int64(nw)

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
	//fmt.Println(url);
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
	rs := string(body);
	res := strings.Split(rs,"|");
	code,_:= strconv.Atoi(res[0]);
	fi := storagedriver.FileInfoFields{
		Path: h.path,
	}
	if code==1 {
		return nil , storagedriver.PathNotFoundError{Path: h.path}
	}else if code !=0{
		return nil,errors.New("get stat err")
	}else{
		isDir,_:= strconv.ParseBool(res[1])
		if(isDir){
			//return &FdfsInfo{Pat:h.path,Isd:true},nil
			fi.IsDir = true
		}else {
			size ,_:= strconv.Atoi(res[3])
			time ,_:=time.Parse("2006-01-02 15:02:05",res[4])
			fi.IsDir = false
			fi.Size = int64(size)
			fi.ModTime = time
			//return &FdfsInfo{Pat:h.path,Isd:false,Siz:int64(size),Mt:time},nil
		}
	}
	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

func (h *Fdfshandle) List()(ss []string,err error){
	url := DK_FDFS_URL+"/list?path="+h.path
	//fmt.Println(url);
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
	rs := string(body);
	json.Unmarshal([]byte(rs),&ss);
	return ss,nil
}

func (h *Fdfshandle) Move(topath string)(err error){
	url := DK_FDFS_URL+"/move?srcpath="+h.path+"&topath="+topath
	//fmt.Println(url);
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
	rs ,_:= strconv.Atoi(string(body));
	if rs == 0 {
		return nil
	}else {
		return errors.New("move fail ~~");
	}
}

func (h *Fdfshandle) Delete()(err error){
	url := DK_FDFS_URL+"/delete?path="+h.path
	//fmt.Println(url);
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
	rs ,_:= strconv.Atoi(string(body));
	if rs == 0 {
		return nil
	}else {
		return errors.New("delete fail ~~");
	}
}

