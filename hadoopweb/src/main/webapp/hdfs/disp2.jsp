<%@page import="org.apache.hadoop.fs.LocatedFileStatus"%>
<%@page import="org.apache.hadoop.fs.RemoteIterator"%>
<%@page import="org.apache.hadoop.fs.FileSystem"%>
<%@page import="org.apache.hadoop.conf.Configuration"%>
<%@page import="org.apache.hadoop.fs.Path"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%-- webapp/hdfs/disp2.jsp --%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>하둡을 이용하여 파일 목록 보기</title>
</head>
<body>
<%
	//하둡서버에 저장된 파일 목록 조회하기
	Configuration conf = new Configuration();
	String root = "D:/20200914/hadoophome/workspace/hadoopstudy1/";
	String path = request.getParameter("path");
	Path dir = new Path(root+path);
	FileSystem fs = FileSystem.get(conf);	//하툽시스템에 저장된 파일 정보
%>
<h2>하둡을 이용한 파일 목록 조회</h2>
<%
	//path로 지정된 폴더의 하위 파일 목록 리턴
	RemoteIterator<LocatedFileStatus> flist = fs.listLocatedStatus(dir);
	while(flist.hasNext()){
		LocatedFileStatus lfs = flist.next();
		if(lfs.isDirectory()){
%>			
<a href="disp2.jsp?path=<%=path+"/"+lfs.getPath().getName() %>">
d--<%=lfs.getPath().getName() %></a><br>
<%		} else { %>
<a href="disp3.jsp?file=<%=lfs.getPath().getName() %>&path=<%=root+path%>">
---<%=lfs.getPath().getName() %></a><br>
<%		}
	}	
%>
</body>
</html>