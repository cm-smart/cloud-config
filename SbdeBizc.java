package com.sgcc.pms.swid.bpbjjcpt.sbde.bizc;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.sgcc.uap.persistence.IHibernateDao;
import com.sgcc.uap.rest.support.QueryFilter;
import com.sgcc.uap.rest.support.QueryResultObject;
import com.sgcc.uap.rest.support.RequestCondition;
import com.sgcc.uap.rest.utils.RestUtils;

@Service
@SuppressWarnings("unchecked")
public class SbdeBizc implements ISbdeBizc{

	private Log log = LogFactory.getLog(SbdeBizc.class);
	
	@Autowired
	private IHibernateDao hibernateDao;

	@Transactional
	public List selectSbde(Map<String, String> params){
		String filter = "where 1=1";
		if(params != null && !params.equals("")){
			Set<Entry<String, String>> keyValues = params.entrySet();
			Iterator<Entry<String, String>> iterator = keyValues.iterator();
			
			while(iterator.hasNext()){
				Entry<String, String> entry = iterator.next();
				String key = entry.getKey();
				String value = entry.getValue();
				if(value != null && !"".equals(value)){
					filter = "and "+key+"="+value;
				}
				
			}
		}
		
		log.info("filter:"+filter);
		
		//更新备品备件定额表sql集合
		List<String> updateBpbjdeSqlList = new ArrayList<String>();
		//查询备品备件定额表
		String bpbjdeSql = "select obj_id,sblx,dydj,desl,kcsl from scyw.T_DW_ZCGL_BPBJDE";
		//查询备品备件大纲包,查询库存数量
		String bpbjdgSql = null;
		//库存数量
		int bpbjdgCount = 0;
		//需补货数量
		int xbhslCount = 0;
		
		List<Object[]> bpbjdeList = hibernateDao.executeSqlQuery(bpbjdeSql);
		if(bpbjdeList != null && bpbjdeList.size() > 0){
			
			String objId = null;
			String sblx = null;
			String dydj = null;
			String desl = null;
			String kcsl = null;
			
			for(int i = 0;i < bpbjdeList.size();i++){
				List bpbjdgList = new ArrayList();
				
				objId = bpbjdeList.get(i)[0].toString();
				sblx = bpbjdeList.get(i)[1].toString();
				dydj = bpbjdeList.get(i)[2].toString();
				desl = bpbjdeList.get(i)[3].toString();
				kcsl = bpbjdeList.get(i)[4].toString();
				//bpbjczzt：备品备件处置状态（可用01、转再利用02、调出03、报废04、待报废05、报废审批中06）；sfysy: 是否已使用
				bpbjdgSql = "select count(1) from scyw.T_SB_BPBJ_BPBJDG where sblx=? and dydj=? and bpbjczzt='01' and sfysy='0'";
				bpbjdgList = hibernateDao.executeSqlQuery(bpbjdgSql, new Object[]{sblx,dydj});

				
				if(bpbjdgList != null && bpbjdgList.size() > 0){
					
					bpbjdgCount = Integer.parseInt(bpbjdgList.get(0).toString());//库存数量
					
					xbhslCount = Integer.parseInt(desl)-bpbjdgCount;//需补货数量
					String updateBpbjdeSql = "update scyw.T_DW_ZCGL_BPBJDE set kcsl='"+bpbjdgCount+"',xbhsl='"+xbhslCount+"' where obj_id='"+objId+"'";
					//添加到集合中
					updateBpbjdeSqlList.add(updateBpbjdeSql);
				}
			}
			//执行updateBpbjdeSqlList集合中的sql语句
			if(updateBpbjdeSqlList != null && updateBpbjdeSqlList.size() > 0){
				String[] sqls = updateBpbjdeSqlList.toArray(new String[updateBpbjdeSqlList.size()]);
				hibernateDao.batchUpdateWithSql(sqls);
			}
		}
		
		//获取定额数量大于库存数量的备品备件定额记录的sql语句
		String sql = "select b.obj_id,b.sblx,b.dydj,(SELECT GG.DMMC  FROM T_DW_BZZX_GGDMB GG WHERE GG.BZFLBM = '010401' AND GG.DM=b.DYDJ) dymc,b.desl,b.kcsl,s.sblxmc from scyw.T_DW_ZCGL_BPBJDE b left join scyw.T_DW_BZZX_SBFL s on b.sblx=s.sblxbm where desl>kcsl";
		List<Map<String, Object>> result = hibernateDao.queryForListWithSql(sql);
		
		return result;
	}

	/**
	 * 多条件查询备品备件定额
	 */
	@Override
	public QueryResultObject selectSbdeMxByparams(RequestCondition requestCondition) throws Exception{
		/*******************统计*********************************/
		//tjBpbjDg();
		
		/**************从bpbjde表中获取数据***********************/
		String sql = "select ROWNUM R,b.obj_id,b.sblx,b.dydj,(SELECT GG.DMMC  FROM T_DW_BZZX_GGDMB GG WHERE GG.BZFLBM = '010401' AND GG.DM=b.DYDJ) dydjmc,nvl(b.desl,0) desl,nvl(b.kcsl,0) kcsl,b.xbhsl xbhsl,b.jldw jldw,b.bz bz,s.sblxmc,b.gldw gldw,i.bmmc gldwmc from scyw.T_DW_ZCGL_BPBJDE b left join scyw.T_DW_BZZX_SBFL s on b.sblx=s.sblxbm LEFT JOIN scyw.ISC_SPECIALORG_UNIT_LOCEXT i on b.gldw=i.ISC_ID where 1=1 ";
		
		List<QueryFilter> wheres = requestCondition.getQueryFilter();
		List<Object> params = new ArrayList<Object>();
		String where = wrapFilter(wheres, params);
		String sql_count = "select count(1) from scyw.T_DW_ZCGL_BPBJDE b where 1=1 ".concat(where);
		String sql_select = sql.concat(where);
		int count = hibernateDao.queryForIntWithSql(sql_count, params.toArray());
		if(count > 0){
			sql_select = wrapPage(sql_select, requestCondition);
			sql_select = "select * from (" + sql_select + ") a ";
			log.info("分页查询数据sql语句:"+sql_select);
			List<Map<String, String>> list = hibernateDao.queryForListWithSql(sql_select, params.toArray());
			return RestUtils.wrappQueryResult(list, count);
		}
		
		return RestUtils.wrappQueryResult(null);
	}
	
	@Override
	public QueryResultObject selectSbdeMxByparamsTj(RequestCondition requestCondition)throws Exception {
		/*******************统计*********************************/
		//tjBpbjDg();
		
		/**************从bpbjde表中获取数据***********************/
		String sql = "select ROWNUM R,b.obj_id,b.sblx,b.dydj,(SELECT GG.DMMC  FROM T_DW_BZZX_GGDMB GG WHERE GG.BZFLBM = '010401' AND GG.DM=b.DYDJ) dydjmc,nvl(b.desl,0) desl,nvl(b.kcsl,0) kcsl,b.xbhsl xbhsl,b.jldw jldw,b.bz bz,s.sblxmc,b.gldw gldw,i.bmmc gldwmc from scyw.T_DW_ZCGL_BPBJDE b left join scyw.T_DW_BZZX_SBFL s on b.sblx=s.sblxbm LEFT JOIN scyw.ISC_SPECIALORG_UNIT_LOCEXT i on b.gldw=i.ISC_ID where 1=1 and b.kcsl >= b.desl";
		
		List<QueryFilter> wheres = requestCondition.getQueryFilter();
		List<Object> params = new ArrayList<Object>();
		String where = wrapFilter(wheres, params);
		String sql_count = "select count(1) from scyw.T_DW_ZCGL_BPBJDE b where 1=1 and  b.kcsl >= b.desl ".concat(where);
		String sql_select = sql.concat(where);
		int count = hibernateDao.queryForIntWithSql(sql_count, params.toArray());
		if(count > 0){
			sql_select = wrapPage(sql_select, requestCondition);
			//sql_select = "select * from (" + sql_select + ") a where a.kcsl >= a.desl";
			log.info("分页查询数据sql语句:"+sql_select);
			List<Map<String, String>> list = hibernateDao.queryForListWithSql(sql_select, params.toArray());
			return RestUtils.wrappQueryResult(list, count);
		}
		
		return RestUtils.wrappQueryResult(null);
	}
	
	/**
	 * 拼装参数
	 */
	private String wrapFilter(List<QueryFilter> wheres,List<Object> params){
		if(wheres != null && wheres.size() > 0){
			int len = wheres.size();
			StringBuffer sqlWhe = new StringBuffer();
			Map<String, Object> temp = new HashMap<String,Object>(len);
			QueryFilter qf = null;
			for(int i = 0;i < len;i++){
				qf = wheres.get(i);
				temp.put(qf.getFieldName(), qf.getValue());
			}
			if(temp.containsKey("SBLX")){
				String sblx = temp.get("SBLX").toString();
				sqlWhe.append(" AND b.SBLX IN (").append(sblx.replaceAll("\\w+", "?")).append(")");
                params.addAll( Arrays.asList(sblx.split(",")));
			}
			if(temp.containsKey("DYDJ")){
				String dydj = temp.get("DYDJ").toString();
				sqlWhe.append(" AND b.DYDJ IN (").append(dydj.replaceAll("\\w+", "?")).append(")");
				params.addAll(Arrays.asList(dydj.split(",")));
			}
			if(temp.containsKey("BGDW")){
				String bgdw = temp.get("BGDW").toString();
				sqlWhe.append(" AND b.GLDW IN (").append(bgdw.replaceAll("\\w+", "?")).append(")");
				params.addAll(Arrays.asList(bgdw.split(",")));
			}
			
			if(temp.containsKey("bgdw")){
				String bgdw = temp.get("bgdw").toString();
				//sqlWhe.append(" and b.PATHID = ?");
				sqlWhe.append(" and b.SJBMID LIKE ?");
				params.addAll( Arrays.asList(bgdw.split(",")));
			}
			if(temp.containsKey("dwjb")){
				String dwjb = temp.get("dwjb").toString();
				sqlWhe.append(" and b.DWJB = ?");
				params.addAll( Arrays.asList(dwjb.split(",")));
			}
			if(temp.containsKey("sblx")){
				String sblx = temp.get("sblx").toString();
				sqlWhe.append(" and a.SBLX in (").append(sblx.replaceAll("\\w+", "?")).append(")");
				params.addAll(Arrays.asList(sblx.split(",")));
			}
			if(temp.containsKey("dydj")){
				String dydj = temp.get("dydj").toString();
				sqlWhe.append(" and a.DYDJ in (").append(dydj.replaceAll("\\w+", "?")).append(")");
				params.addAll(Arrays.asList(dydj.split(",")));
			}
			return sqlWhe.toString();
		}
		
		return "";
	}
	
	/**
	 * 拼接分页
	 */
	private String wrapPage(String sql,RequestCondition rc){
		int pSize = 2000, pIdx = 1;
        try {
        	if(rc.getPageSize() != null){
        		pSize = Integer.parseInt(rc.getPageSize());
        	}
            if(rc.getPageIndex() != null){
            	pIdx = Integer.parseInt(rc.getPageIndex());
            }
            
        } catch (Exception e) { 
        	log.info("获取分页信息失败", e);
        }
        StringBuilder sbu = new StringBuilder();
        if (pIdx <= 1) {
        	sbu.append(sql).append(" AND ROWNUM <= ").append(pSize);
        } else {
        	sbu.append("SELECT * FROM ( ").append(sql).append(" AND ROWNUM <= ")
        		.append(pSize * pIdx).append(") A WHERE A.R > ").append(pSize * (pIdx - 1));
        }
        return sbu.toString();
	}

	
	
	/**
	 * 保存更新
	 */
	@Override
	public Map saveOrUpdate(List<Map> list) throws Exception{
		
		Map<String, String> result = new HashMap<String, String>();
		
		if(list == null || list.size() == 0){
			result.put("msg", "参数为空");
			return result;
		}
		
		try{
			
			for(int i = 0;i < list.size();i++){
				Map map = list.get(i);
				
				String sblx = (String)map.get("sblx");
				String dydj = (String)map.get("dydj");
				int desl = (Integer)map.get("desl");
				String jldw = (String)map.get("jldw");
				String bz = (String)map.get("bz");
				
				if(map.containsKey("objId")){
					//更新
					String id = (String)map.get("objId");
					
					String update_sql = "update scyw.T_DW_ZCGL_BPBJDE set sblx=?,dydj=?,desl=?,jldw=?,bz=? where obj_id=?";
					
					log.info(update_sql);
					
					hibernateDao.updateWithSql(update_sql,new Object[]{sblx,dydj,desl,jldw,bz,id});
					
					result.put("msg", "success");
					
				}else{
					//新增
					//先根据sblx和dydj查询是否存在
					String select_sql = "select count(1) from scyw.T_DW_ZCGL_BPBJDE where sblx=? and dydj=?";
					int isExist = hibernateDao.queryForIntWithSql(select_sql,new Object[]{sblx,dydj});
					if(isExist > 0){
						result.put("msg", "数据库已经存在该定额的维护数据");
					}else{
						StringBuffer insert_sql = new StringBuffer("insert into scyw.T_DW_ZCGL_BPBJDE (obj_id,sblx,dydj,desl,jldw,bz) values (sys_guid(),");
						insert_sql.append("'"+sblx+"'").append(",");
						insert_sql.append("'"+dydj+"'").append(",");
						insert_sql.append("'"+desl+"'").append(",");
						insert_sql.append("'"+jldw+"'").append(",");
						insert_sql.append("'"+bz+"'").append(")");
						
						log.info(insert_sql.toString());
						
						hibernateDao.updateWithSql(insert_sql.toString());
						
						result.put("msg", "success");//成功
					}
					
				}
			}
		}catch (Exception e) {
			throw e;
		}
		
		
		return result;
	}

	/**
	 * 删除
	 */
	@Override
	public void delete(String objId) throws Exception {
		
		if(objId == null || objId.equals("")){
			throw new Exception("参数objId为空！");
		}
		
		String sql_delete = "delete from scyw.T_DW_ZCGL_BPBJDE where obj_id=?";
		
		hibernateDao.updateWithSql(sql_delete, new Object[]{objId});
	}

	/**
	 * 根据pathId和dwjb查询所属单位
	 */
	@Override
	public QueryResultObject selectXjdwByPathId(RequestCondition requestCondition) throws Exception{
		
		String sql = "SELECT ROWNUM R,b.ISC_ID,b.BMMC FROM ISC_SPECIALORG_UNIT_LOCEXT b WHERE 1=1";
		//String sql = "SELECT ROWNUM R,b.ISC_ID,b.BMMC,a.*,s.sblxmc,d.dmmc dydjmc FROM ISC_SPECIALORG_UNIT_LOCEXT b LEFT JOIN T_DW_ZCGL_BPBJDE a on b.ISC_ID=a.gldw left join scyw.T_DW_BZZX_SBFL s on a.sblx=s.sblxbm left join scyw.T_DWZY_SY_DYDJ d on a.dydj=d.dm where 1=1";
		List<QueryFilter> wheres = requestCondition.getQueryFilter();
		List<Object> params = new ArrayList<Object>();
		String where = wrapFilter(wheres, params);
		
		if(where.contains("SBLX")){
			sql = "SELECT ROWNUM R,b.ISC_ID,b.BMMC,a.*,s.sblxmc,(SELECT GG.DMMC  FROM T_DW_BZZX_GGDMB GG WHERE GG.BZFLBM = '010401' AND GG.DM=a.DYDJ) dydjmc FROM ISC_SPECIALORG_UNIT_LOCEXT b LEFT JOIN T_DW_ZCGL_BPBJDE a on b.ISC_ID=a.gldw left join scyw.T_DW_BZZX_SBFL s on a.sblx=s.sblxbm where 1=1";
		}
		
		String sql_count = "select count(1) from scyw.ISC_SPECIALORG_UNIT_LOCEXT b LEFT JOIN T_DW_ZCGL_BPBJDE a on b.ISC_ID=a.gldw where 1=1 ".concat(where);
		String sql_select = sql.concat(where);
		int count = hibernateDao.queryForIntWithSql(sql_count, params.toArray());
		if(count > 0){
			sql_select = wrapPage(sql_select, requestCondition);
			log.info("分页查询数据sql语句:"+sql_select);
			List<Map<String, String>> list = hibernateDao.queryForListWithSql(sql_select, params.toArray());
			
			return RestUtils.wrappQueryResult(list, count);
		}
		
		return RestUtils.wrappQueryResult(null);
	}
	
	/**
	 * 多条件查询当前单位和子单位的设备定额信息
	 */
	@Override
	public QueryResultObject selectXjdwByParams(RequestCondition requestCondition) throws Exception {
		String sql_select = "SELECT ROWNUM R,b.ISC_ID,b.BMMC,a.*,s.sblxmc,(SELECT GG.DMMC  FROM T_DW_BZZX_GGDMB GG WHERE GG.BZFLBM = '010401' AND GG.DM=a.DYDJ) dmmc FROM ISC_SPECIALORG_UNIT_LOCEXT b LEFT JOIN T_DW_ZCGL_BPBJDE a on b.ISC_ID=a.gldw left join scyw.T_DW_BZZX_SBFL s on a.sblx=s.sblxbm";
		List<QueryFilter> wheres = requestCondition.getQueryFilter();
		List<Object> params = new ArrayList<Object>();
		String where = wrapFilter(wheres, params);
		String sql_count = "select count(1) from scyw.ISC_SPECIALORG_UNIT_LOCEXT b where 1=1 ".concat(where);
		sql_select = sql_select.concat(where);
		int count = hibernateDao.queryForIntWithSql(sql_count, params.toArray());
		if(count > 0){
			sql_select = wrapPage(sql_select, requestCondition);
			log.info("分页查询数据sql语句:"+sql_select);
			List<Map<String, String>> list = hibernateDao.queryForListWithSql(sql_select, params.toArray());
			
			return RestUtils.wrappQueryResult(list, count);
		}
		
		return RestUtils.wrappQueryResult(null);
	}
	
	/**
	 * 根据保管单位，设备类型，电压等级查询备品备件定额信息
	 */
	@Override
	public List selectSbdeByParams(Map<String, String> params) throws Exception{
		
		List<Map<String, Object>> mapList = new ArrayList<Map<String,Object>>();
		
		try {
			String sql_select = "select * from scyw.T_DW_ZCGL_BPBJDE where GLDW=? AND SBLX=? AND DYDJ=?";
			
			mapList = hibernateDao.queryForListWithSql(sql_select, params.values().toArray());
			
			//获取该单位的下级单位的设备定额信息
			String sql_selectXjdwDesl = "select * from scyw.T_DW_ZCGL_BPBJDE where sblx = ? and DYDJ=? and gldw in (SELECT I.ISC_ID FROM ISC_SPECIALORG_UNIT_LOCEXT I WHERE I.SJBMID LIKE ? AND I.DWJB IN ( SELECT sum( GLJB + 1 ) AS gljb FROM ISC_SPECIALORG_UNIT_LOCEXT WHERE ISC_ID = ? ))";
			List<Map<String, Object>> xjdwDexx = hibernateDao.queryForListWithSql(sql_selectXjdwDesl, new Object[]{params.get("sblx"),params.get("dydj"),params.get("bgdw"),params.get("bgdw")});
			//将子单位的设备定额信息放到mapList中，返回给前端
			mapList.addAll(xjdwDexx);
		} catch (Exception e) {
			throw e;
		}
		
		
		return mapList;
	}

	/**
	 * 保存或修改备品备件定额信息
	 */
	@Override
	public Map saveOrUpdateSbde(List<Map> list) throws Exception{
		Map<String, String> result = new HashMap<String, String>();
		
		if(list == null || list.size() == 0){
			result.put("msg", "参数为空");
			return result;
		}
		
		try{
			
			for(int i = 0;i < list.size();i++){
				Map map = list.get(i);
				
				String sblx = (String)map.get("sblx");
				String dydj = (String)map.get("dydj");
				Object desl = map.get("desl");
				String gldw = (String)map.get("bgdw");
				String jldw = (String)map.get("jldw");
				
				if(map.containsKey("objId") && map.get("objId") != null &&! map.get("objId").equals("")){
					//更新
					String id = (String)map.get("objId");
					
					String update_sql = "update scyw.T_DW_ZCGL_BPBJDE set sblx=?,dydj=?,desl=?,GLDW=?,jldw=? where obj_id=?";
					
					log.info(update_sql);
					
					hibernateDao.updateWithSql(update_sql,new Object[]{sblx,dydj,desl,gldw,jldw,id});
					
					result.put("msg", "success");
					
				}else{
					//新增
					//先根据sblx和dydj查询是否存在
					String select_sql = "select count(1) from scyw.T_DW_ZCGL_BPBJDE where sblx=? and dydj=? and gldw=?";
					int isExist = hibernateDao.queryForIntWithSql(select_sql,new Object[]{sblx,dydj,gldw});
					if(isExist > 0){
						result.put("msg", "数据库已经存在该定额的维护数据");
					}else{
						StringBuffer insert_sql = new StringBuffer("insert into scyw.T_DW_ZCGL_BPBJDE (obj_id,sblx,dydj,desl,gldw,jldw) values (sys_guid(),");
						insert_sql.append("'"+sblx+"'").append(",");
						insert_sql.append("'"+dydj+"'").append(",");
						insert_sql.append("'"+desl+"'").append(",");
						insert_sql.append("'"+gldw+"'").append(",");
						insert_sql.append("'"+jldw+"'").append(")");
						
						log.info(insert_sql.toString());
						
						hibernateDao.updateWithSql(insert_sql.toString());
						
						result.put("msg", "success");//成功
					}
					
				}
			}
		}catch (Exception e) {
			throw e;
		}
		
		
		return result;
	}

	/**
	 * 批量删除
	 */
	@Override
	public void deleteBatch(List<String> list) throws Exception {
		try {
			if(list == null || list.size() == 0){
				throw new Exception("list参数为空！");
			}
			//首先根据obj_id获取设备定额信息，然后再根据gldw，sblx,dydj查询子单位的设备定额信息，然后删除
			String sql_select = "SELECT * from scyw.T_DW_ZCGL_BPBJDE WHERE OBJ_ID = ?";
			String sql_delete_self = "delete from scyw.T_DW_ZCGL_BPBJDE where obj_id=?";
			
			log.info(sql_select);
			for(String str:list){
				Map<String, String> map = hibernateDao.queryForMapWithSql(sql_select, new Object[]{str});
				log.info(map);
				//根据gldw，sblx,dydj删除子单位的设备定额信息
				String sql_delete = "delete from scyw.T_DW_ZCGL_BPBJDE where 1=1 and sblx = ? and dydj = ? and gldw in (SELECT I.ISC_ID FROM ISC_SPECIALORG_UNIT_LOCEXT I WHERE I.SJBMID=? AND I.DWJB IN ( SELECT sum( GLJB + 1 ) AS gljb FROM ISC_SPECIALORG_UNIT_LOCEXT WHERE ISC_ID = ? ))";
				log.info(sql_delete);
				hibernateDao.executeSqlUpdate(sql_delete, new Object[]{map.get("SBLX"),map.get("DYDJ"),map.get("GLDW"),map.get("GLDW")});
				//删除本记录
				hibernateDao.executeSqlUpdate(sql_delete_self,new Object[]{str});
			}
			
			
		} catch (Exception e) {
			throw e;
		}
	}

	/**
	 * 根据保管单位，设备类型，电压等级查询查询该单位的子单位的设配定额信息
	 */
	@Override
	public List selectXjdwFpxxByParam(Map<String, String> params) throws Exception {
		if(params == null || params.size() == 0){
			throw new Exception("参数为空");
		}
		
		try {
			//1.先根据保管单位查询子单位的isc_id和bmmc
			String sql_select_i = "SELECT ROWNUM R,i.ISC_ID,i.BMMC FROM ISC_SPECIALORG_UNIT_LOCEXT i WHERE 1=1 and i.sjbmid = ? and i.dwjb in (SELECT sum( GLJB + 1 ) AS gljb FROM ISC_SPECIALORG_UNIT_LOCEXT WHERE ISC_ID = ?)";
			List<Map<String, String>> mapList = hibernateDao.queryForListWithSql(sql_select_i, new Object[]{params.get("GLDW"),params.get("GLDW")});
			for(int i=0;i<mapList.size();i++){
				mapList.get(i).put("SBLX", params.get("SBLX"));
				mapList.get(i).put("SBLXMC", params.get("SBLXMC"));
				mapList.get(i).put("DYDJ", params.get("DYDJ"));
				mapList.get(i).put("DYDJMC", params.get("DYDJMC"));
			}
			//2.根据保管单位，设备类型，电压等级查询查询该单位的子单位的设配定额信息
			String sql_select_s = "SELECT ROWNUM R,b.ISC_ID,b.BMMC,a.*,s.sblxmc,(SELECT GG.DMMC  FROM T_DW_BZZX_GGDMB GG WHERE GG.BZFLBM = '010401' AND GG.DM=a.DYDJ) dydjmc FROM ISC_SPECIALORG_UNIT_LOCEXT b LEFT JOIN T_DW_ZCGL_BPBJDE a on b.ISC_ID=a.gldw left join scyw.T_DW_BZZX_SBFL s on a.sblx=s.sblxbm where 1=1 and a.GLDW in (SELECT I.ISC_ID FROM ISC_SPECIALORG_UNIT_LOCEXT I WHERE I.SJBMID=? AND I.DWJB IN ( SELECT sum( GLJB + 1 ) AS gljb FROM ISC_SPECIALORG_UNIT_LOCEXT WHERE ISC_ID = ? )) AND a.SBLX=? AND a.dydj=?";
			List<Map<String, String>> mapList_sbde = hibernateDao.queryForListWithSql(sql_select_s, new Object[]{params.get("GLDW"),params.get("GLDW"),params.get("SBLX"),params.get("DYDJ")});
			
			//遍历子单位信息,和设备定额信息，将没有该设备类型和电压等级的子单位信息封装
			if(mapList_sbde == null || mapList_sbde.size() == 0){
				return mapList;
			}else{
				List<Map<String, String>> result = new ArrayList<Map<String,String>>();
				
				for(int i=0;i<mapList.size();i++){
					Map<String, String> temp = mapList.get(i);
					
					for(int j=0;j<mapList_sbde.size();j++){
						if(mapList_sbde.get(j).get("ISC_ID").equals(temp.get("ISC_ID"))){
							temp = mapList_sbde.get(j);
							break;
						}
					}
					result.add(temp);
				}
				return result;
			}
			
		} catch (Exception e) {
			throw e;
		}
	}
	
	/**
	 * 根据gldw,sblx,dydj统计kcsl，插入到bpbjde表中
	 */
	private void tjBpbjDg() throws Exception{
		
		//更新备品备件定额表sql集合
		List<String> updateBpbjdeSqlList = new ArrayList<String>();
		//更新备品备件定额表sql
		String updateBpbjdeSql = "";
		int bpbjdgCount = 0;
		long xbhslCount = 0;
		
		//查询备品备件定额表
		String bpbjdeSql = "select obj_id,sblx,dydj,desl,gldw from scyw.T_DW_ZCGL_BPBJDE WHERE gldw is not null";
				
		List<Map<String, Object>> mapList = hibernateDao.queryForListWithSql(bpbjdeSql);
		if(mapList != null && mapList.size() > 0){
			for(Map<String, Object> temp:mapList){
				String objId = temp.get("OBJ_ID").toString();
				String desl = "0";
				if(temp.get("DESL") != null){
					desl = temp.get("DESL").toString();
				}
				String gldw = temp.get("GLDW").toString();
				String sblx = temp.get("SBLX").toString();
				String dydj = temp.get("DYDJ").toString();
				
				String bpbjdgSql = "select count(1) from scyw.T_SB_BPBJ_BPBJDG where gldw=? and sblx=? and dydj=? and bpbjczzt='01' and sfysy='0'";
				List bpbjdglist = hibernateDao.executeSqlQuery(bpbjdgSql,new Object[]{gldw,sblx,dydj});
				if(!bpbjdglist.isEmpty()){
					bpbjdgCount = Integer.parseInt(bpbjdglist.get(0).toString());
					xbhslCount = Long.parseLong(desl)-bpbjdgCount;
					updateBpbjdeSql = "update scyw.T_DW_ZCGL_BPBJDE set kcsl='"+bpbjdgCount+"',xbhsl='"+xbhslCount+"' where obj_id='"+objId+"'";
					updateBpbjdeSqlList.add(updateBpbjdeSql);
				}
			}
		}
		//执行updateBpbjdeSqlList集合中的sql语句
		if(updateBpbjdeSqlList != null && updateBpbjdeSqlList.size() > 0){
			String[] sqls = updateBpbjdeSqlList.toArray(new String[updateBpbjdeSqlList.size()]);
			hibernateDao.batchUpdateWithSql(sqls);
		}
	}

	/**
	 * 根据dwid查询单位信息
	 */
	@Override
	public Object selectDwxxByDwid(String dwid) throws Exception{
		
		try {
			if(dwid != null && dwid != ""){
				String sql = "SELECT I.ISC_ID,I.BMMC,I.DWJB FROM ISC_SPECIALORG_UNIT_LOCEXT I WHERE I.ISC_ID = ?";
				return hibernateDao.queryForMapWithSql(sql, new Object[]{dwid});
			}else{
				throw new Exception("dwid为空！");
			}
		} catch (Exception e) {
			throw e;
		}
	}

}
