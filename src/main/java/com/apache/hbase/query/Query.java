/**
 * 
 */
package com.apache.hbase.query;

import java.util.List;

/**
 * HBase查询客户端接口
 * 
 * @author zhangfeng
 *
 */
public interface Query {
	
	/**
	 * 根据条件查询数据
	 * @param query 查询对象
	 * @return
	 */
	public List<String> query(QueryObject query);

    /**
     * 根据rowkey获取对应的FSN图片字节码数据
     * @param rowkey
     * @return
     */

	
}
