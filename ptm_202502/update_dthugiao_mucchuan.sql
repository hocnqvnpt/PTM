
select * from
	ttkd_bsc.dinhmuc_giao_dthu_ptm
	where thang = 202502 and loai_kpi = 'KPI_NV' and ma_vtcv in (select MA_VTCV
						from ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang = 202502);and ma_nv in ('VNP030414', 'CTV080676', 'VNP017647', 'VNP017423', 'VNP031191');
	and loai_kpi in ('KPI_CHT', 'KPI_CHT_GDV', 'KPI_GD', 'KPI_TT', 'KPI_TL', 'KPI_TCA', 'KPI_PGD'); 
	
	----Update cột TONG_DTHU, KHDK để tính BSC theo XEPHANG_P1 đối với nhân viên chỉ tiêu DTHU tháng
	MERGE INTO ttkd_bsc.dinhmuc_giao_dthu_ptm a
	USING (select MA_VTCV, TEN_VTCV, THANG, DINHMUC_1, DINHMUC_2, DINHMUC_3, DINHMUC_4, VANBAN
						from ttkd_bsc.bldg_danhmuc_vtcv_p1 where vanban like '%thang%') b
	ON (a.thang = b.thang 
					and (case when a.ma_pb in ('VNP0702400', 'VNP0702500') and a.ma_vtcv = 'VNP-HNHCM_KHDN_3' 
										then 'VNP-HNHCM_KHDN_3_23' else a.ma_vtcv end) = b.ma_vtcv
			)
	WHEN MATCHED THEN
		UPDATE SET
						TONG_DTGIAO = case when XEPHANG_P1 in (4, 5) then b.DINHMUC_2
																else b.DINHMUC_4 end
--						, KHDK = case when XEPHANG_P1 in (4, 5) then b.DINHMUC_2
--																else b.DINHMUC_4 end
--	select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a
	WHERE a.thang = 202502 and XEPHANG_P1 in (1, 2, 3, 4, 5) and loai_kpi = 'KPI_NV'
					and a.ma_vtcv in (select MA_VTCV
													from ttkd_bsc.bldg_danhmuc_vtcv_p1 where vanban like '%thang%' and  thang = a.thang)
--													and ma_nv in (select ma_nv from hocnq_ttkd.x_nv_tmp where XEPHANG_P1 is not null)
--				and ma_nv = 'VNP016764'

	;
	----Update cột DINHMUC 1,2,3 tương ứng Vượt chuẩn, Chuẩn, Trung bình để tính đon giá theo XEPHANG_P1 đối với nhân viên chỉ tiêu DTHU tháng
	MERGE INTO ttkd_bsc.dinhmuc_giao_dthu_ptm a
	USING (select MA_VTCV, TEN_VTCV, THANG, DINHMUC_1, DINHMUC_2, DINHMUC_3, VANBAN
						from ttkd_bsc.bldg_danhmuc_vtcv_p1 where vanban like '%thang%') b
	ON (a.thang = b.thang 
					and (case when a.ma_pb in ('VNP0702400', 'VNP0702500') and a.ma_vtcv = 'VNP-HNHCM_KHDN_3' 
										then 'VNP-HNHCM_KHDN_3_23' else a.ma_vtcv end) = b.ma_vtcv
			)
	WHEN MATCHED THEN
		UPDATE SET
						a.DINHMUC_1 = b.DINHMUC_1
						, a.DINHMUC_2 = b.DINHMUC_2
						, a.DINHMUC_3 = b.DINHMUC_3
--	select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a								
	WHERE a.thang = 202502 --and XEPHANG_P1 in (1, 2, 3, 4, 5) --and loai_kpi in ('KPI_NV', 'KPI_CHT_GDV')
--				and a.ma_nv in ('VNP030414', 'CTV080676', 'VNP017647', 'VNP017423', 'VNP031191')
					and a.ma_vtcv in (select MA_VTCV
													from ttkd_bsc.bldg_danhmuc_vtcv_p1 where vanban like '%thang%' and  thang = a.thang)
--and a.ma_nv in (select ma_nv from hocnq_ttkd.x_ds_lech_p1)
			
	;
	
	update ttkd_bsc.dinhmuc_giao_dthu_ptm a set DINHMUC_1 = null, DINHMUC_2 = null, DINHMUC_3 = null where thang = 202502;
	commit;
	
	rollback;
	
	
	--------Update cột GIAO để tính BSC theo XEPHANG_P1 đối với nhân viên chỉ tiêu DTHU năm
	MERGE INTO ttkd_bsc.dthu_ptm_vtcntt_2025_giao a
	USING (select *
								from ttkd_bsc.dthu_ptm_vtcntt_2025_mucgiao) b
	ON ((case when a.ma_pb in ('VNP0702400', 'VNP0702500') and a.ma_vtcv = 'VNP-HNHCM_KHDN_3' 
										then 'VNP-HNHCM_KHDN_3_23'
						when a.ma_pb in ('VNP0702400', 'VNP0702500') and a.ma_vtcv = 'VNP-HNHCM_KHDN_23' 
										then 'VNP-HNHCM_KHDN_23_23'else a.ma_vtcv end) = b.ma_vtcv 
			and (case when a.XEPHANG_P1 in (4, 5) then 'chuan'
								when a.XEPHANG_P1 in (1, 2, 3) then 'min' else null end) = b.MUC
			)
	WHEN MATCHED THEN
		UPDATE SET
						a.GIAO = case when a.XEPHANG_P1 in (4, 5) and b.MUC = 'chuan' and a.SOTHANG_VTCV = 2 then b.LUYKE_202502 * 1000000
												when a.XEPHANG_P1 in (4, 5) and b.MUC = 'chuan' and a.SOTHANG_VTCV = 1 then b.LUYKE_202501 * 1000000
												when a.XEPHANG_P1 in (1, 2, 3) and b.MUC = 'min' and a.SOTHANG_VTCV = 2 then b.LUYKE_202502 * 1000000
												when a.XEPHANG_P1 in (1, 2, 3) and b.MUC = 'min' and a.SOTHANG_VTCV = 1 then b.LUYKE_202501 * 1000000
										else 0 end
--		select sum(giao) from ttkd_bsc.dthu_ptm_vtcntt_2025_giao a
		WHERE a.thang = 202502 and a.XEPHANG_P1 in (1, 2, 3, 4, 5) and a.vitri = 'NV' --and a.ma_nv in ('VNP017694', 'VNP029156')
						and a.ma_kpi='HCM_DT_PTMOI_066' 
						and a.ma_vtcv in (select MA_VTCV
													from ttkd_bsc.dthu_ptm_vtcntt_2025_mucgiao)
--													and a.ma_nv in (select ma_nv from hocnq_ttkd.x_ds_lech_p1)
					
;
commit;
rollback;

update ttkd_bsc.dthu_ptm_vtcntt_2025_giao set GIAO = null
 where thang=202502 and ma_kpi='HCM_DT_PTMOI_066' and VITRI = 'NV'
 ;
---document 
	select *
  from ttkd_bsc.dthu_ptm_vtcntt_2025_mucgiao
  ;

select *
  from ttkd_bsc.dthu_ptm_vtcntt_2025_giao 
 where thang=202502 and ma_kpi='HCM_DT_PTMOI_066'; and VITRI = 'TT'
 ;
 select * from ttkd_bsc.dthu_ptm_vtcntt_2025_thuchien;	--duy tri các dvu khác
 
 select * from ttkd_bsc.man_ct_duytri_vnptt --> duy tri VNPtt;
 
 MERGE INTO ttkd_bsc.dthu_ptm_vtcntt_2025_giao a
	USING (select * from ttkd_bsc.dinhmuc_giao_dthu_ptm) b
	ON ( a.thang = b.thang and a.ma_nv = b.ma_nv
			)
	WHEN MATCHED THEN
		UPDATE SET
						a.XEPHANG_P1 = b.XEPHANG_P1
--		select * from ttkd_bsc.dthu_ptm_vtcntt_2025_giao a
		WHERE a.thang = 202502 and a.XEPHANG_P1 is null and a.vitri = 'NV'
						and a.ma_kpi='HCM_DT_PTMOI_066' 
						and a.ma_vtcv in (select MA_VTCV
													from ttkd_bsc.dthu_ptm_vtcntt_2025_mucgiao where MUC = 'chuan')
;
commit;

rollback;

create table bangluong_kpi_202502 as select * from ttkd_bsc.bangluong_kpi a where thang = 202502
													and ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_065', 'HCM_DT_PTMOI_069')
													;
													select * from bangluong_kpi_202502
										minus
										select * from ttkd_bsc.bangluong_kpi a where thang = 202502
													and ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_065', 'HCM_DT_PTMOI_069');
													
														
										select * from ttkd_bsc.bangluong_kpi a where thang = 202502
													and ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_065', 'HCM_DT_PTMOI_069')
													minus
													select * from bangluong_kpi_202502
										
	
	

