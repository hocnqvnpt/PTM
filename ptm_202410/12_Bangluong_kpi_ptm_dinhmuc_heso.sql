
	---Cac nhom tra sau - Ca Nhan
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a
		   set NHOMVINATS_KQTH = (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_trasau_canhan  where ma_nv = a.ma_nv and loaitb_id = 20)
				 , NHOMBRCD_KQTH = (select round(sum(dthu_kpi), 0)
																								from ttkd_bsc.temp_trasau_canhan a1
																										where ma_nv = a.ma_nv 
																													and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																								)
				, NHOMCNTT_KQTH = (select round(sum(dthu_kpi), 0)
																			from ttkd_bsc.temp_trasau_canhan a1
																					where ma_nv = a.ma_nv 
																								and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																			)
				, NHOMCONLAI_KQTH = (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_trasau_canhan a1
																										where ma_nv = a.ma_nv
																										and not loaitb_id = 20
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																)
--			select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a
			where thang = 202409
	;
	---To truong
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a
		   set NHOMVINATS_KQTH= (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_totruong where ma_to = a.ma_to and loaitb_id = 20)
				, NHOMBRCD_KQTH = (select round(sum(dthu_kpi), 0)
																				from ttkd_bsc.temp_totruong a1
																						where exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																									and ma_to = a.ma_to 
																				) 
				, NHOMCNTT_KQTH = (select round(sum(dthu_kpi), 0)
																			from ttkd_bsc.temp_totruong a1
																					where exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																								and ma_to = a.ma_to
																			)
				, NHOMCONLAI_KQTH = (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_totruong a1
																										where ma_to = a.ma_to
																										and not loaitb_id = 20
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																)
--		  select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a
			 where a.thang = 202409
							and exists (select dthu_kpi from ttkd_bsc.temp_totruong where ma_to = a.ma_to)
							and exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
												where ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_062') and thang = a.thang and to_truong_pho = 1 and ma_vtcv = a.ma_vtcv)
	;
	----Lanh dao Phong
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a
		   set NHOMVINATS_KQTH = (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_021_ldp where DICHVU = 'VNP tra sau' and ma_nv = a.ma_nv)
				, NHOMBRCD_KQTH = (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_021_ldp  where DICHVU in ('Mega+Fiber', 'MyTV') and ma_nv = a.ma_nv)
				, NHOMCNTT_KQTH = (select round(sum(dthu_kpi), 0) from ttkd_bsc.temp_021_ldp  where DICHVU in ('Dich vu so doanh nghiep') and ma_nv = a.ma_nv)
				, NHOMCONLAI_KQTH = (select round(sum(dthu_kpi), 0) 
																				from ttkd_bsc.temp_021_ldp a1
																					where   a1.DICHVU in ('Con lai')
																									and ma_nv = a.ma_nv)
--		  select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a
		where  a.thang = 202409
					and exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_062') and thang = a.thang and giamdoc_phogiamdoc = 1 and ma_vtcv=a.ma_vtcv)
					and exists(select * from ttkd_bsc.temp_021_ldp where ma_nv = a.ma_nv) 
					;      
	
	commit;
	rollback;
	---END
			;	
	
	select * from
	ttkd_bsc.dinhmuc_giao_dthu_ptm
	where thang = 202409 and ma_nv in ('VNP016902',
'VNP017072',
'VNP017853',
'VNP017014',
'VNP016898',
'VNP019529');
--							'VNP017722'
--				and loai_kpi = 'KPI_CHT_GDV'
--					and ten_to like '%Tr? Tr??c%'
				and ten_nv like '%Kh�nh Linh%'
	;
	commit;
	rollback;

select sum(HESO_QD_DT_PTM) from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = 202407; 485.8
select * from ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang = 202409;
select * from ttkd_bsc.nhanvien where thang = 202407; and ten_to like '%Tr? Tr??c';
select * from ttkdhcm_ktnv.ID430_TTDANGKY_CHOTTHANG where thang = 202407 
;
select * from
	ttkd_bsc.bangluong_kpi
	where thang = 202409 and ma_kpi in ('HCM_TB_GIAHA_024', 'HCM_TB_GIAHA_026')
				and ma_nv in ('VNP019931','VNP017585');ma_pb = 'VNP0701600'
	;
