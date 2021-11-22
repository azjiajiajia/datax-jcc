/**
 * @(#)DomainUtil.java, 11月 16, 2021.
 * <p>
 * Copyright 2021 pinghang.com. All rights reserved.
 * PINGHANG.COM PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.alibaba.datax.common.util;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h2></h2>
 * @author jcc
 * date 2021/11/16
 */
public class DomainUtil {
    public static String getDomain(String strContent) {
        String strDomain = strContent;

        Pattern pattern = Pattern.compile("(http|ftp|https):\\/\\/[\\w\\-_]+(\\.[\\w\\-_]+)+([\\w\\-\\.,@?^=%&:/~\\+#]*[\\w\\-\\@?^=%&/~\\+#])?");
        Matcher matcher = pattern.matcher(strContent);
        if (matcher.find()) {
            if (strContent.startsWith("http://"))
                strContent = strContent.replace("http://", "");
            if (strContent.startsWith("https://"))
                strContent = strContent.replace("https://", "");
            if (strContent.startsWith("www."))
                strContent = strContent.replace("www.", "");
            if (strContent.indexOf("/") > 0)
                strContent = strContent.substring(0, strContent.indexOf("/"));

            strDomain = strContent;
            List<String> domains = Arrays.asList(".net.cn", ".com.cn", ".gov.cn", ".cn", ".com", ".net",
                    ".org", ".so", ".co", ".mobi", ".tel", ".biz", ".info", ".name", ".me", ".cc", ".tv", ".asiz", ".hk");
            for (int i = 0; i < domains.size(); ++i) {
                String strDomainItem = domains.get(i);
                if (strContent.indexOf(strDomainItem) > 0 && strContent.indexOf(strDomainItem) + strDomainItem.length() >= strContent.length()) {
                    strContent = strContent.replace(strDomainItem, "");
                    if (strContent.lastIndexOf(".") > 0)
                        strContent = strContent.substring(strContent.lastIndexOf(".") + 1);
                    strDomain = strContent + strDomainItem;
                    break;
                }
            }
        }
        return strDomain;
    }
}
