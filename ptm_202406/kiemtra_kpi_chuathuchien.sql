-- Kiem tra chua co kpi
begin
ttkd_bsc.checknull_bangluong_kpi;
end;

select a.*, b.ten_Kpi
  from ttkd_bsc.temp_bangluong_kpi a
			join blkpi_danhmuc_kpi b on b.thang_kt is null and a.ma_kpi = b.ma_kpi
			;
  
  select * from ttkd_bsc.bangluong_kpi_202405 where HCM_TB_GIAHA_024 is not null;
  
  HCM_SL_DAILY_003	    Công tác phát tri?n m?i ??i lý m?i ??ng th?i có phát sinh doanh thu trong tháng phát tri?n
 HCM_SL_BRVNP_001	S?n l??ng phát tri?n m?i BRC?, VNP tr? sau
 HCM_DT_PTMOI_055	Doanh thu d?ch v? di ??ng tr? tr??c phát tri?n m?i có duy trì trong n?m
HCM_DT_PTNAM_006	T?ng doanh thu phát tri?n m?i các d?ch v? trong n?m

