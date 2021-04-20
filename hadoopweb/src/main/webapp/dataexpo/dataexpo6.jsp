<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions"%>
<%-- webapp/dataexpo/dataexpo5.jsp --%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>항공사별 도착/출발 지연 분석(정시/지연/조기), 운항거리분석 : 막대그래프작성하기</title>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script>
<script type="text/javascript"
 src="https://www.chartjs.org/dist/2.9.4/Chart.min.js"></script>
</head>
<body>
<h3>항공사별 도착/출발 지연 분석</h3>
<form action="${pageContext.request.contextPath}/CarrMultiServlet2" method="post" name="f">
	<select name="year">
		<c:forEach var="y" begin="1987" end="1988">
			<option <c:if test="${param.year == y}">selected</c:if>>${y}</option>
		</c:forEach>
	</select>&nbsp;&nbsp;&nbsp;
	<input type="radio" name="kbn" value="d" checked>추발&nbsp;&nbsp;
	<input type="radio" name="kbn" value="a"
			<c:if test="${param.kbn=='d'}">checked</c:if>>도착&nbsp;&nbsp;
	<input type="submit" value="데이터분석">
</form>
<c:if test="${!empty file}">
	<div id="canvas-holder" style="width:70%;">
		<canvas id="chart" width="100%"></canvas>
	</div>
	<script type="text/javascript">
		var randomColorFactor = function(){
			return Math.round(Math.random()*255);
		}
		var randomColor = function(opacity){
			return "rgba(" + randomColorFactor() + ","
					+ randomColorFactor() + ","
					+ randomColorFactor() + ","
					+ (opacity || ".3") + ")";
		}
		arrcolor = new Array("rgb(255,0,0)","rgb(0,255,0)","rgb(0,0,255)");
		var kbn = "${param.kbn=='s'?'정시':param.kbn=='d'?'지연':'조기'}";
		var label = new Array(kbn+"출발건수",kbn+"도착건수","운항거리(1000마일)");
		var config = {
				type : "bar",
				data : {
					datasets : [
						<c:forEach items="${list}" var="map" varStatus="stat">
						{ 
						label :'${file}년'+label[${stat.index}], 
						data:[<c:forEach items="${map}" var="m">"${m.value}",</c:forEach>],
						backgroundColor : [<c:forEach items="${map}" var="m">arrcolor[${stat.index}],</c:forEach>],
						},
						</c:forEach> ],
						labels : [<c:forEach items="${list[0]}" var="m">
								"${fn:split(m.key,'-')[1]}월",</c:forEach>]
						},
					options : {
						responsive : true
					}
		};
		window.onload = function(){
			var ctx = document.getElementById("chart").getContext("2d");
			new Chart(ctx,config);
		}
	</script>
</c:if>
</body>
</html>