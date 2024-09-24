create table ttkd_bsc.temp_vnptt as
with dthu_hoamang as(
			    select manv_ptm ma_nv, ma_to_ptm ma_to, mapb_ptm ma_pb, ma_vtcv_ptm, sum(doanhthu_kpi_nvptm) dthu_kpi_ptm, 'TT' dichvu
			    , (select distinct  ma_nv from ttkd_bsc.blkpi_dm_to_pgd b where thang=202407 and b.ma_to = a.ma_to_ptm and DICHVU ='VNP tra truoc') ma_nv_pgd
			    from manpn.manpn_goi_tonghop_202407 a 
			    where doanhthu_kpi_nvptm>0 and thoadk_bsc=1 and loai_tb = 'ptm'
			    group by manv_ptm, ma_to_ptm, mapb_ptm,ma_vtcv_ptm 
		)
, dthu_goi as(
			    select manv_ptm ma_nv, ma_to_ptm ma_to, mapb_ptm ma_pb, ma_vtcv_ptm, sum(doanhthu_kpi_nvptm) dthu_kpi_ptm, 'TT' dichvu
			    , (select distinct  ma_nv from ttkd_bsc.blkpi_dm_to_pgd b where thang=202407 and b.ma_to=a.ma_to_ptm and DICHVU ='VNP tra truoc') ma_nv_pgd
			    from manpn.manpn_goi_tonghop_202407 a
			    where doanhthu_kpi_nvptm>0 and thoadk_bsc=1 and loai_tb = 'ptm_goi'
			    group by manv_ptm, ma_to_ptm, mapb_ptm,ma_vtcv_ptm 
		)
, ds_nhanvien as(
			    select a.*, 'HHM' nguon from dthu_hoamang a
			    union all
			    select b.*, 'GOI' nguon from dthu_goi b
			)
select MA_NV, MA_TO, MA_PB, MA_VTCV_PTM, ma_nv_pgd, NGUON, sum(DTHU_KPI_PTM) dthu
from ds_nhanvien a 
group by MA_NV, MA_TO, MA_PB, MA_VTCV_PTM, ma_nv_pgd, NGUON
;
create table ttkd_bsc.temp_vnptt_to as
	select MA_TO, MA_PB, NGUON, sum(DTHU) dthu
	from ttkd_bsc.temp_vnptt a
	group by MA_TO, MA_PB, NGUON
	;
create table ttkd_bsc.temp_vnptt_pb as
	select MA_NV_PGD, MA_PB, NGUON, sum(DTHU) dthu
	from ttkd_bsc.temp_vnptt
	group by  MA_NV_PGD, MA_PB, NGUON
	;
		 
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a 
				set KQTH = (select round(sum(dthu), 0) from ttkd_bsc.temp_vnptt where ma_nv = a.ma_nv)
				where a.thang = 202407
				;
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a 
				set KQTH = (select round(sum(dthu), 0) from ttkd_bsc.temp_vnptt_pb where MA_NV_PGD = a.ma_nv)
				where a.thang = 202407
				;
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a 
				set KQTH = (select round(sum(dthu), 0) from ttkd_bsc.temp_vnptt_to where ma_to = a.ma_to)
				where a.thang = 202407
							and loai_kpi = 'KPI_TT'
				;
	
	---Cac nhom tra sau - Ca Nhan
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a
		   set NHOMVINATS_KQTH = 1000000* (select sum(dthu_kpi) from ttkd_bsc.temp_trasau_canhan  where ma_nv = a.ma_nv and loaitb_id = 20)
				 , NHOMBRCD_KQTH = 1000000*(select sum(dthu_kpi)
																								from ttkd_bsc.temp_trasau_canhan a1
																										where ma_nv = a.ma_nv 
																													and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																								)
				, NHOMCNTT_KQTH = 1000000*(select sum(dthu_kpi)
																			from ttkd_bsc.temp_trasau_canhan a1
																					where ma_nv = a.ma_nv 
																								and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																			)
				, NHOMCONLAI_KQTH = 1000000*(select sum(dthu_kpi) from ttkd_bsc.temp_trasau_canhan a1
																										where ma_nv = a.ma_nv
																										and not loaitb_id = 20
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																)
			where thang = 202407
	;
	---To truong
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a
		   set NHOMVINATS_KQTH= 1000000*(select sum(dthu_kpi) from ttkd_bsc.temp_totruong where ma_to = a.ma_to and loaitb_id = 20)
				, NHOMBRCD_KQTH = 1000000*(select sum(dthu_kpi)
																				from ttkd_bsc.temp_totruong a1
																						where exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																									and ma_to = a.ma_to 
																				) 
				, NHOMCNTT_KQTH = 1000000*(select sum(dthu_kpi)
																			from ttkd_bsc.temp_totruong a1
																					where exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																								and ma_to = a.ma_to
																			)
				, NHOMCONLAI_KQTH = 1000000*(select sum(dthu_kpi) from ttkd_bsc.temp_totruong a1
																										where ma_to = a.ma_to
																										and not loaitb_id = 20
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																)
--		  select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a
			 where exists (select dthu_kpi from ttkd_bsc.temp_totruong where ma_to=a.ma_to) 
							and thang = 202407
									and exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
												where ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_053') and thang = 202407 and to_truong_pho = 1 and ma_vtcv=a.ma_vtcv)
	;
	----Lanh dao Phong
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a
		   set NHOMVINATS_KQTH = 1000000*(select sum(dthu_kpi) from ttkd_bsc.temp_021_ldp where DICHVU = 'VNP tra sau' and ma_nv = a.ma_nv)
				, NHOMBRCD_KQTH = 1000000*(select sum(dthu_kpi) from ttkd_bsc.temp_021_ldp  where DICHVU in ('Mega+Fiber', 'MyTV') and ma_nv = a.ma_nv)
				, NHOMCNTT_KQTH = 1000000*(select sum(dthu_kpi) from ttkd_bsc.temp_021_ldp  where DICHVU in ('Dich vu so doanh nghiep') and ma_nv = a.ma_nv)
				, NHOMCONLAI_KQTH = 1000000*(select sum(dthu_kpi) 
																				from ttkd_bsc.temp_021_ldp a1
																					where   a1.NHOM_DICHVU in ('Dichvu_Con lai')
																									and ma_nv = a.ma_nv)
		where  exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_053') and thang = 202407 and giamdoc_phogiamdoc = 1 and ma_vtcv=a.ma_vtcv)
					and exists(select * from ttkd_bsc.temp_021_ldp where ma_nv = a.ma_nv)
					;      
	
	
	---END
			;	
	
	select * from
	ttkd_bsc.dinhmuc_giao_dthu_ptm
	where thang = 202407 --and ma_nv = 'CTV021946'
--						= 'VNP019515';
--							'VNP017722'
--				and loai_kpi = 'KPI_CHT_GDV'
--					and ten_to like '%Tr? Tr??c%'
				and ten_nv like '%Khánh Linh%'
	;
	commit;
	rollback;

select sum(HESO_QD_DT_PTM) from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = 202407; 485.8
select * from ttkd_bsc.nhanvien where thang = 202407; and ten_to like '%Tr? Tr??c';
select * from ttkdhcm_ktnv.ID430_TTDANGKY_CHOTTHANG where thang = 202407 
;
select * from
	ttkd_bsc.bangluong_kpi
	where thang = 202407
	;
