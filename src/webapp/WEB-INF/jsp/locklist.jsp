<c:choose>
	<c:when test="${empty locks}">
<f:message key="locklist.nolocks"/>
	</c:when>
	<c:otherwise>
<table>
<tr><th><f:message key="common.topic"/></th><th><f:message key="locklist.lockedat"/></th></tr>
		<c:forEach items="${locks}" var="lock">
<tr>
	<td class="recent"><a href="<jmwiki:link var="${lock.topicName}" />"><c:out value="${lock.topicName}"/></a></td>
	<td class="recent"><f:formatDate value="${lock.time}" type="both" dateStyle="MEDIUM" timeStyle="MEDIUM" /></td>
	<td><a href="<jmwiki:link var="Special:Unlock" />?topic=<jmwiki:encode value="${lock.topicName}" />"><f:message key="locklist.unlock"/></a></td>
</tr>
		</c:forEach>
</table>
	</c:otherwise>
</c:choose>
