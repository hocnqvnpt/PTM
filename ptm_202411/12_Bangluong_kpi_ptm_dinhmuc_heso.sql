
	---Cac nhom tra sau - Ca Nhan
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a
		   set NHOMVINATS_KQTH = (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_trasau_canhan  where ma_nv = a.ma_nv and loaitb_id = 20 and nguon <> 'vnp_qd')
				 , NHOMBRCD_KQTH = (select round(sum(dthu_kpi), 0)
																								from ttkd_bsc.temp_trasau_canhan a1
																										where ma_nv = a.ma_nv and nguon <>  'vnp_qd'
																													and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																								)
				, NHOMCNTT_KQTH = (select round(sum(dthu_kpi), 0)
																			from ttkd_bsc.temp_trasau_canhan a1
																					where ma_nv = a.ma_nv and nguon <>  'vnp_qd'
																								and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																			)
				, NHOMCONLAI_KQTH = (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_trasau_canhan a1
																										where ma_nv = a.ma_nv and nguon <>  'vnp_qd'
																										and not (loaitb_id = 20 and nguon <> 'vnp_qd')
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																)
				, NHOMVNPQD_KQTH =  (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_trasau_canhan  where ma_nv = a.ma_nv and nguon =  'vnp_qd')
--			select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a
			where thang = 202411
	;
	---To truong
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a
		   set NHOMVINATS_KQTH= (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_totruong where ma_to = a.ma_to and loaitb_id = 20 and nguon <>  'vnp_qd')
				, NHOMBRCD_KQTH = (select round(sum(dthu_kpi), 0)
																				from ttkd_bsc.temp_totruong a1
																						where exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																									and ma_to = a.ma_to and nguon <>  'vnp_qd'
																				) 
				, NHOMCNTT_KQTH = (select round(sum(dthu_kpi), 0)
																			from ttkd_bsc.temp_totruong a1
																					where exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																								and ma_to = a.ma_to and nguon <>  'vnp_qd'
																			)
				, NHOMCONLAI_KQTH = (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_totruong a1
																										where ma_to = a.ma_to and nguon <>  'vnp_qd'
																										and not (loaitb_id = 20 and nguon <> 'vnp_qd')
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																)
				, NHOMVNPQD_KQTH =  (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_totruong a1 where ma_to = a.ma_to and nguon =  'vnp_qd')
--		  select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a
			 where a.thang = 202411
							and exists (select dthu_kpi from ttkd_bsc.temp_totruong where ma_to = a.ma_to)
							and exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
												where ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_062') and thang = a.thang and to_truong_pho = 1 and ma_vtcv = a.ma_vtcv)
	;
	----Lanh dao Phong
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a
		   set NHOMVINATS_KQTH = (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_021_ldp where PHUTRACH = 'VNP tra sau' and ma_nv = a.ma_nv and nguon <>  'vnp_qd')
				, NHOMBRCD_KQTH = (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_021_ldp  where PHUTRACH in ('Mega+Fiber', 'MyTV') and ma_nv = a.ma_nv and nguon <>  'vnp_qd')
				, NHOMCNTT_KQTH = (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_021_ldp  where PHUTRACH in ('Dich vu so doanh nghiep') and ma_nv = a.ma_nv and nguon <>  'vnp_qd')
				, NHOMCONLAI_KQTH = (select round(sum(dthu_kpi), 0) 
																				from ttkd_bsc.temp_021_ldp a1
																					where   a1.PHUTRACH in ('Con lai')
																									and ma_nv = a.ma_nv and nguon <>  'vnp_qd')
				, NHOMVNPQD_KQTH =  (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_021_ldp where ma_nv = a.ma_nv and nguon =  'vnp_qd')
--		  select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a
		where  a.thang = 202411
					and exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_062') and thang = a.thang and giamdoc_phogiamdoc = 1 and ma_vtcv=a.ma_vtcv)
					and exists(select * from ttkd_bsc.temp_021_ldp where ma_nv = a.ma_nv) 
					;      
	
	commit;
	rollback;
	---END
			;	
	select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_062')
									and thang = 202411;
	select * from
	ttkd_bsc.dinhmuc_giao_dthu_ptm
	where thang = 202411 and ma_vtcv  in ('VNP-HNHCM_KHDN_3.1', 'VNP-HNHCM_KHDN_18', 'VNP-HNHCM_KHDN_3') 
						and ma_pb in (
											'VNP0702300',
											'VNP0702400',
											'VNP0702500'
											) 
											;
--							'VNP017722'
--				and loai_kpi = 'KPI_CHT_GDV'
--					and ten_to like '%Tr? Tr??c%'
				and ten_nv like '%Kh�nh Linh%'
	;
	commit;
	rollback;

select sum(HESO_QD_DT_PTM) from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = 202407; 485.8
select * from ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang = 202411;
select * from ttkd_bsc.nhanvien where thang = 202407; and ten_to like '%Tr? Tr??c';
select * from ttkdhcm_ktnv.ID430_TTDANGKY_CHOTTHANG where thang = 202407 
;
drop table ttkd_bsc.x_dinhmuc_giao_dthu_ptm_202411;
create table ttkd_bsc.x_dinhmuc_giao_dthu_ptm_202411 as select * from
	ttkd_bsc.dinhmuc_giao_dthu_ptm
	where thang = 202411
	;
select * from
	ttkd_bsc.bangluong_kpi
	where thang = 202411 and ma_kpi in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_026')
				and ma_nv in ('VNP019931','VNP017585');ma_pb = 'VNP0701600'
	;