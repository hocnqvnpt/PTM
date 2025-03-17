	---COde UPDATE TYLE_SD Fiber---
	select * from x_tmp;
	drop table x_tmp purge;
	create table x_tmp as
	with km_sd as (select a.thuebao_id, max(a.tyle_sd) tyle_sd 
								from css.v_khuyenmai_dbtb@dataguard a, css.v_ct_khuyenmai@dataguard b
								  where a.chitietkm_id = b.chitietkm_id and a.thang_bd >= 202501 and a.thang_bd = 202501 		---thang n
												and a.hieuluc = 1 and ttdc_id = 0 and a.tyle_sd>0 and a.tyle_sd<>100
												and a.khuyenmai_id not in (1977, 2056, 2998, 2999) 
--												and (b.nhom_km in (1,12) or b.nhom_km is null) 
								  group by thuebao_id
							)
--		select * from km_sd
--		where thuebao_id = '12282290'
		
		select a.TYLE_SD, a.TYLE_TB, a.thuebao_id, ma_tb, DICH_VU, DTHU_PS, DTHU_GOI_GOC, DTHU_GOI, b.TYLE_SD TYLE_SD_n
		from ttkd_bct.ptm_codinh_202501 a
					join km_sd b on a.thuebao_id = b.thuebao_id
		where dichvuvt_id = 4 and loaitb_id in (58, 59) and a.TYLE_SD is not null and b.TYLE_SD is not null
		;
		update ttkd_bct.ptm_codinh_202501 a set a.tyle_sd =  (select TYLE_SD_n from x_tmp where thuebao_id = a.thuebao_id)
--		select ma_tb, tyle_sd from ttkd_bct.ptm_codinh_202501 a
		where dichvuvt_id = 4 and loaitb_id in (58, 59) and a.TYLE_SD is  null
--				and ma_tb not in (select ma_tb from x_tmp)
		;
		commit;
	---END

select * from hocnq_ttkd.x_br_ck;
--- nêu có goi_dadv_id thì ưu tiên cột nvl(DTHU_GOI_TT, DTHU_GOI_DADV) dthu_goi_skm
---nếu không có goi_dadv thì lấy cột DTHU_GOI_SKM đên hêt tháng 10, sau tháng 11 sử dung cột DTHU_GOI (vì đã tính ck sẵn rôi)
---Thang 6 trơ di da bsung tien tru dan đe loại bỏ tiên Green Net
;
drop table hocnq_ttkd.x_br_ck_202407_l1;
create table hocnq_ttkd.x_br_ck_202411_l1 as
with tbl as---thang n-1
			(select thuebao_id, tien_td, tien_sd, 'km_dbtb' nguon, ngay_sd 
					from css_hcm.khuyenmai_dbtb where hieuluc = 1 and ttdc_id = 0 and tien_td > 0 and to_number(to_char(ngay_sd, 'yyyymm')) >= 202410
				union all
				select thuebao_id, tien_td, tien_sd, 'db_datcoc', ngay_cn from css_hcm.db_datcoc where hieuluc = 1 and ttdc_id = 0 and tien_td > 0 and to_number(to_char(ngay_cn, 'yyyymm')) >= 202410
			)
		, dthu_skm as
			(
			select a.*, row_number() over(partition by thuebao_id order by ngay_sd) rnk
				from tbl a --where thuebao_id = 12236669
			)
		, f_dadv as (select goi_id, 58 loaitb_id, ten_goi, FIBER + DATA + THOAI_NM as dthu_goi_dadv from bcss_hcm.banggia_goi where NGAY_HIEU_LUC is null)
		, tv_dadv as (select goi_id, 61 loaitb_id, ten_goi, MYTV + VTVCAB + SPOTV as dthu_goi_dadv from bcss_hcm.banggia_goi where NGAY_HIEU_LUC is null)
select db.thuebao_id, db.ma_tb, db.dichvuvt_id, db.loaitb_id, db.goi_id goi_dadv_id, db.dthu_goi, a.tyle_sd, db.dthu_goi * (100 - nvl(a.tyle_sd, 0))/100 dthu_goi_skm--, round((b.tien_td/1.1), 0) dthu_goi_tt
			, b.tien_sd dthu_goi_tt
			, nvl(c.dthu_goi_dadv, c1.dthu_goi_dadv) dthu_goi_dadv--, c.dthu_goi_dadv gf, c1.dthu_goi_dadv gm
from ttkd_bsc.ct_bsc_ptm db
			left join ttkd_bct.ptm_codinh_202411 a on db.hdtb_id = a.hdtb_id
			left join dthu_skm b on a.thuebao_id = b.thuebao_id and b.rnk = 1
			left join f_dadv c on db.goi_id = c.goi_id and db.loaitb_id = c.loaitb_id
			left join tv_dadv c1 on db.goi_id = c1.goi_id and db.loaitb_id = c1.loaitb_id
where db.thang_ptm = 202411 and db.dichvuvt_id = 4 and db.dthu_goi > 0 and db.thang_tldg_dt > 0; and db.ma_tb in ('kimngan8195352')
;
create table hocnq_ttkd.x_vnp_ck_202409 as
with tbl_ps as
			(select '84' || ma_tb ma_tb, sum(NOGOC) NOGOC
				from bcss_hcm.ct_no partition (kc20241101) 
				where loaitb_id = 20 and khoanmuctt_id in (212, 208)
				group by ma_tb)
select db.id, db.thuebao_id, db.ma_tb, db.dichvuvt_id, db.loaitb_id, db.goi_id, db.dthu_goi, a1.NOGOC + a.NOGOC dthu_goi_skm, a1.NOGOC dthu_goi_goc, a.NOGOC tien_ck
from ttkd_bsc.ct_bsc_ptm db
			join bcss_hcm.ct_no partition (kc20241101) a on db.ma_tb = '84' || a.ma_tb and a.loaitb_id = 20 and a.khoanmuctt_id = 3993
			left join tbl_ps a1 on db.ma_tb = a1.ma_tb
where db.thang_ptm = 202409 and db.loaitb_id = 20 and db.dthu_goi > 0 and db.thang_tldg_dt > 0
;
and db.ma_tb in ('84812679205')
;
drop table hocnq_ttkd.x_vnp_ck_202405;
select * from bcss_hcm.ct_no partition (kc20241101) where ma_tb in ('915249680');
select * from bcss_hcm.khoanmuc_tt where khoanmuctt_id in (3993, 212, 208, 201);
select * from v_thongtinkm_all where ma_tb = 'loan0511';
select MA_TD, TOCDO_ID, MUCUOCTB_ID, goi_dadv_id from ttkd_bct.ptm_codinh_202404 where ma_tb = 'hcmny1504app';
select * from ttkd_bsc.ct_bsc_ptm where ma_tb = 'nty12417';

select * from css_hcm.goi_dadv_lhtb where goi_id = '16073';
select * from css_hcm.muccuoc_tb where tocdo_id = '21455';

select * from ttkd_bsc.ct_bsc_ptm  where thang_ptm=202404 and ma_tb in ('ravi217');
select * from ttkd_bsc.ct_bsc_ptm  where thang_ptm = 202411 and loaitb_id = 20 and dthu_goi > 60000 and thang_luong = 71 order by ngay_bbbg desc;

select * from ttkd_bsc.ct_thuhoi_online a
 where thang=202411 and loaitb_id in (58,59,61,171) and thuhoi_ktnv=1 and goi_saukm is null;
 
 create table ttkd_bsc.x_vnp_ck_202502 as
		 with tbl_ps as
					(select ma_tb, sum(NOGOC) NOGOC
						from bcss_hcm.ct_no partition (kc20250201) 		----thang n
						where loaitb_id = 20 and khoanmuctt_id in (212, 208, 201)
						group by ma_tb)
		 select db.ma_hd, db.SOMAY ma_tb, a.NOGOC ck, b.NOGOC ps, round(abs(a.NOGOC)/b.NOGOC, 2) heso_ck
		 from ---thang n
					ttkd_bsc.dt_ptm_vnp_202502 db
						join bcss_hcm.ct_no partition (kc20250201) a on db.SOMAY = '84' || a.ma_tb and db.loaitb_id = a.loaitb_id
						join tbl_ps b on a.ma_tb = b.ma_tb
		 where db.loaitb_id = 20 and a.khoanmuctt_id = 3993 --and a.NOGOC =0 --and b.NOGOC = 0
			--		and db.thang_luong = 71
			;


