select * from ttkd_bsc.v_ct_ptm_kpi_truongline_202405;

CREATE OR REPLACE VIEW ttkd_bsc.v_ct_ptm_kpi_truongline_202405 as
select thang_ptm, ma_nv,ma_to,ma_pb,ma_gd,ma_tb,dich_vu, ngay_sd,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
       ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro,tyle_hotro,manv_tt_dai
       ,sum(dthu_kpi)dthu_kpi 
  from 
(select manv_ptm ma_nv,ma_to,(select ma_pb from ttkd_bsc.nhanvien where thang = 202405 and manv_hrm=a.manv_ptm) ma_pb
        ,ma_gd,ma_tb,dich_vu,to_char(ngay_bbbg,'dd/mm/yyyy') ngay_sd,thang_ptm,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
       ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro,heso_diaban_tinhkhac, tyle_huongdt, heso_hoso
       ,tyle_hotro,manv_tt_dai
       ,thang_tlkpi thang_tlkpi_nv,thang_tlkpi_to,doanhthu_kpi_to dthu_kpi
  from ct_bsc_ptm a
 where thang_tlkpi_to= 202405 and (loaitb_id<>21 or ma_kh='GTGT rieng')
 
 
 union all 
select manv_ptm,ma_to,(select ma_pb from ttkd_bsc.nhanvien where thang = 202405 and manv_hrm=a.manv_ptm) ma_pb
       ,ma_gd,ma_tb,dich_vu,to_char(ngay_bbbg,'dd/mm/yyyy') ngay_sd,thang_ptm,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
       ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro,heso_diaban_tinhkhac, tyle_huongdt, heso_hoso
       ,tyle_hotro,manv_tt_dai
       ,thang_tlkpi thang_tlkpi_nv,thang_tlkpi_to,doanhthu_kpi_dnhm
  from ct_bsc_ptm a
 where thang_tlkpi_dnhm_to=202405
                and (loaitb_id<>21 or ma_kh='GTGT rieng')
                and doanhthu_kpi_dnhm is not null
 
 
 union all 
select manv_tt_dai,ma_to_dai,(select ma_pb from ttkd_bsc.nhanvien where thang = 202405 and manv_hrm=a.manv_tt_dai) ma_pb,
        ma_gd,ma_tb,dich_vu,to_char(ngay_bbbg,'dd/mm/yyyy') ngay_sd,thang_ptm,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
       ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro,heso_diaban_tinhkhac, tyle_huongdt, heso_hoso
       ,tyle_hotro,manv_tt_dai
       ,thang_tlkpi thang_tlkpi_nv,thang_tlkpi_to,doanhthu_kpi_nvdai 
  from ct_bsc_ptm a
 where thang_tlkpi_to=202405
                and (loaitb_id<>21 or ma_kh='GTGT rieng')
                and manv_tt_dai is not null

 union all
select manv_ptm ma_nv,ma_to,(select ma_pb from ttkd_bsc.nhanvien where thang = 202405 and manv_hrm=a.manv_ptm) ma_pb
        ,ma_gd,ma_tb,dich_vu,to_char(ngay_bbbg,'dd/mm/yyyy') ngay_sd,thang_ptm,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
       ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro,heso_diaban_tinhkhac, tyle_huongdt, heso_hoso
       ,tyle_hotro,manv_tt_dai
       ,thang_tlkpi thang_tlkpi_nv,thang_tlkpi_to,doanhthu_kpi_nvptm dthu_kpi
  from ct_bsc_ptm a
 where thang_tlkpi_to=202405 and loaitb_id=21
 
)m

 
 group by m.ma_nv,m.ma_to,ma_pb,ma_gd,ma_tb,dich_vu, ngay_sd,thang_ptm,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
       ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro,heso_diaban_tinhkhac
       ,tyle_huongdt, heso_hoso,tyle_hotro,manv_tt_dai;

