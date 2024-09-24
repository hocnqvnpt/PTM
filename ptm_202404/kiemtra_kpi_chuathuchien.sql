-- Kiem tra chua co kpi
begin
checknull_bangluong_kpi;
end;

select ma_kpi,(select ten_kpi from blkpi_danhmuc_kpi where thang_kt is null and ma_kpi=a.ma_kpi)ten_kpi 
  from ttkd_bsc.temp_bangluong_kpi a;
 

