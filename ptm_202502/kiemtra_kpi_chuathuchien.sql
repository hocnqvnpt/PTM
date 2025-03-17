-- Kiem tra chua co kpi
--begin
--ttkd_bsc.checknull_bangluong_kpi;
--end;
--
--select a.*, b.ten_Kpi
--  from ttkd_bsc.temp_bangluong_kpi a
--			join blkpi_danhmuc_kpi b on b.thang = 202407 and a.ma_kpi = b.ma_kpi
--			;
--  
--  select * from ttkd_bsc.bangluong_kpi_202405 where HCM_TB_GIAHA_024 is not null;
--  
 ;
 
 select a.*
  from ttkd_bsc.blkpi_danhmuc_kpi a
			where thang = 202502 
 ;
 --KIEM TRA đánh dâu dư cột
	  select ma_kpi, ten_kpi, CHITIEU_GIAO + GIAO + THUCHIEN + TYLE_THUCHIEN + MUCDO_HOANTHANH + DIEM_CONG + DIEM_TRU total
	 from ttkd_bsc.blkpi_danhmuc_kpi a
	 where a.thang = 202502
	 ;
 
  --KIEM TRA các giá trị nhập GIAO/CHITIEUGIAO
			 select a.THANG, a.MA_KPI, a.TEN_KPI, a.MA_NV, TEN_NV, MA_VTCV, a.CHITIEU_GIAO, a.GIAO, b.nguoi_xuly
			 from ttkd_bsc.bangluong_kpi a
						join ttkd_bsc.blkpi_danhmuc_kpi b on a.thang = b.thang and a.ma_kpi = b.ma_kpi
			 where a.thang = 202502 and b.giao = 1
			 ;
			select a.THANG, a.MA_KPI, a.TEN_KPI, a.MA_NV, TEN_NV, MA_VTCV, a.CHITIEU_GIAO, a.GIAO, b.nguoi_xuly
			 from ttkd_bsc.bangluong_kpi a
						join ttkd_bsc.blkpi_danhmuc_kpi b on a.thang = b.thang and a.ma_kpi = b.ma_kpi
			 where a.thang = 202502 and b.chitieu_giao = 1
	;
	---KIEM TRA Lanh dao khong giao KPI
	select * from ttkd_bsc.bangluong_kpi a
	where thang = 202502 and ma_vtcv in ('VNP-HNHCM_BHKV_1', 'VNP-HNHCM_BHKV_2.1', 'VNP-HNHCM_BHKV_2')
				and (ma_nv, ma_kpi) not in (select ma_nv, ma_kpi from ttkd_bsc.blkpi_dm_to_pgd where thang = a.thang)
				
	;
	
	 --KIEM TRA các giá trị nhập THUCHIEN/TYLE_THUCHIEN/MDHT/DIEM
			 select a.THANG, a.MA_KPI, a.TEN_KPI, a.MA_NV, TEN_NV, MA_VTCV, a.THUCHIEN, b.nguoi_xuly, a.NGAY_PUBLIC
			 from ttkd_bsc.bangluong_kpi a
						join ttkd_bsc.blkpi_danhmuc_kpi b on a.thang = b.thang and a.ma_kpi = b.ma_kpi
			 where a.thang = 202502 and b.THUCHIEN = 1
			 ;
			select a.THANG, a.MA_KPI, a.TEN_KPI, a.MA_NV, TEN_NV, MA_VTCV, a.TYLE_THUCHIEN, b.nguoi_xuly, a.NGAY_PUBLIC
			 from ttkd_bsc.bangluong_kpi a
						join ttkd_bsc.blkpi_danhmuc_kpi b on a.thang = b.thang and a.ma_kpi = b.ma_kpi
			 where a.thang = 202502 and b.TYLE_THUCHIEN = 1
			 ;
			 select a.THANG, a.MA_KPI, a.TEN_KPI, a.MA_NV, TEN_NV, MA_VTCV, a.MUCDO_HOANTHANH, b.nguoi_xuly, a.NGAY_PUBLIC
			 from ttkd_bsc.bangluong_kpi a
						join ttkd_bsc.blkpi_danhmuc_kpi b on a.thang = b.thang and a.ma_kpi = b.ma_kpi
			 where a.thang = 202502 and b.MUCDO_HOANTHANH = 1
			 ;
			 select a.THANG, a.MA_KPI, a.TEN_KPI, a.MA_NV, TEN_NV, MA_VTCV, a.DIEM_cong, a.DIEM_tru, b.nguoi_xuly, a.NGAY_PUBLIC
			 from ttkd_bsc.bangluong_kpi a
						join ttkd_bsc.blkpi_danhmuc_kpi b on a.thang = b.thang and a.ma_kpi = b.ma_kpi
			 where a.thang = 202502 and (b.DIEM_cong = 1 or b.DIEM_tru = 1)
			 ;
	;

