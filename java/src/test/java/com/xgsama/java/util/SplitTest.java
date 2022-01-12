package com.xgsama.java.util;

/**
 * SplitTest
 *
 * @author : xgSama
 * @date : 2021/10/26 09:57:17
 */
public class SplitTest {
    public static void main(String[] args) {
        String str = "id:string,xh:string,ydlx_id:string,fwlx_id:string,clwh:string,sqsj:date,bgsj:date,kssj:date,jzsj:date,ysxm:string,yszjhm:string,ysmz_id:string,ysxb_id:string,ysdw_id:string,ysxlcc_id:string,ylqpc:string,ysyx_id:string,ysx_id:string,yszy_id:string,ysnj:string,ysjt:string,xsxm:string,xszjhm:string,xsmz_id:string,xsdw_id:string,xsxlcc_id:string,xlqpc:string,gkzf:string,zrdwzdf:string,xsyx_id:string,xsx_id:string,xszy_id:string,xsnj:string,xsjt:string,ydsm:string,sqzt:string,bgbh:string,deleted:string,czr_userid:string,xsxb_id:string,yds_id:string,xds_id:string,ydsy:string,sxsj:date,fwsj:date,xz:bigint,dqsj:date,ycssj:date,xcssj:date,yzzmm:string,xzzmm:string,yqlx_id:string,fxsj:string";
        String[] split = str.split(",");
        for (String s : split) {
            System.out.println(s);
            if (s.split(":")[0].equalsIgnoreCase("SFYXSH")) {
                System.out.println(")))))");
            }
        }

        System.out.println(split.length);


        
        String str1 = "jzsj:date,kssj:date,sfyxsh:string,sqsj:date,sqzt:string,sxsj:date,xds_id:string,xh:string,xlqpc:string,xsdw_id:string,xsjt:string,xsmz_id:string,xsnj:string,xsx_id:string,xsxb_id:string,xsxlcc_id:string,xsxm:string,xsyx_id:string,xszjhm:string,xszy_id:string,xz:double,ydlx_id:string,yds_id:string,ydsm:string,ydsy:string,ylqpc:string,ysdw_id:string,ysjt:string,ysmz_id:string,ysnj:string,ysx_id:string,ysxb_id:string,ysxlcc_id:string,ysxm:string,ysyx_id:string,yszjhm:string,yszy_id:string,zrdwzdf:string,ycssj:timestamp,xcssj:timestamp,yzzmm:string,xzzmm:string,yqlx_id:string";
        System.out.println(str1.split(",").length);

    }
}
