select * from ttkd_bsc.v_ct_ptm_kpi_truongline_202407;
drop view ttkd_bsc.v_ct_ptm_kpi_truongline_202407;

CREATE OR REPLACE VIEW ttkd_bsc.v_ct_ptm_kpi_truongline_202407 as
select thang_ptm, ma_nv,ma_to,ma_pb,ma_gd,ma_tb,dich_vu, ngay_sd,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
       ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro, heso_hotro_nvptm, heso_hotro_nvhotro, manv_tt_dai
       ,sum(dthu_kpi)dthu_kpi 
  from 
(select manv_ptm ma_nv, ma_to, ma_pb
        ,ma_gd,ma_tb,dich_vu,to_char(ngay_bbbg,'dd/mm/yyyy') ngay_sd,thang_ptm,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
       ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro,heso_diaban_tinhkhac, tyle_huongdt, heso_hoso
       , heso_hotro_nvptm, heso_hotro_nvhotro, manv_tt_dai
       ,thang_tlkpi thang_tlkpi_nv,thang_tlkpi_to,doanhthu_kpi_to dthu_kpi
  from ttkd_bsc.ct_bsc_ptm a
 where thang_tlkpi_to= 202407 and (loaitb_id<>21 or ma_kh='GTGT rieng')
 
 
 union all 
		---To  Nvien Cot MANV_PTM cho dthu DNHM
				select manv_ptm, ma_to, ma_pb
					  ,ma_gd,ma_tb,dich_vu,to_char(ngay_bbbg,'dd/mm/yyyy') ngay_sd,thang_ptm,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
					  ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro,heso_diaban_tinhkhac, tyle_huongdt, heso_hoso
					  , heso_hotro_nvptm, heso_hotro_nvhotro, manv_tt_dai
					  ,thang_tlkpi thang_tlkpi_nv,thang_tlkpi_to, doanhthu_kpi_dnhm * heso_hotro_nvptm as doanhthu_kpi_dnhm
				  from ttkd_bsc.ct_bsc_ptm a
				 where thang_tlkpi_dnhm_to=202407
							 and (loaitb_id<>21 or ma_kh='GTGT rieng')
							 and doanhthu_kpi_dnhm is not null
		---To  Nvien Cot MANV_HOTRO cho dthu DNHM
			union all
				select manv_hotro ma_nv, b.ma_to, b.ma_pb
							,ma_gd,ma_tb,dich_vu,to_char(ngay_bbbg,'dd/mm/yyyy') ngay_sd,thang_ptm,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
							  ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro,heso_diaban_tinhkhac, tyle_huongdt, heso_hoso
							  , heso_hotro_nvptm, heso_hotro_nvhotro, manv_tt_dai
							, thang_tlkpi_hotro thang_tlkpi_nv, thang_tlkpi_to
							, doanhthu_kpi_dnhm * heso_hotro_nvhotro as doanhthu_kpi_dnhm
					  from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on a.thang_tlkpi_dnhm_to = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_dnhm_to=202407 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null
									and doanhthu_kpi_dnhm >0
 
 union all 
select manv_tt_dai, b.ma_to, b.ma_pb,
				   ma_gd,ma_tb,dich_vu,to_char(ngay_bbbg,'dd/mm/yyyy') ngay_sd,thang_ptm,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
				  ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro,heso_diaban_tinhkhac, tyle_huongdt, heso_hoso
				  , heso_hotro_nvptm, heso_hotro_nvhotro, manv_tt_dai
				  ,thang_tlkpi thang_tlkpi_nv, thang_tlkpi_to, doanhthu_kpi_nvdai 
  from ttkd_bsc.ct_bsc_ptm a
			join ttkd_bsc.nhanvien b on a.thang_tlkpi_dnhm_to = b.thang and a.manv_tt_dai = b.ma_nv
 where thang_tlkpi_to=202407
                and (loaitb_id<>21 or ma_kh='GTGT rieng')
                and manv_tt_dai is not null

 union all
		----VNPtt
select manv_ptm ma_nv,ma_to, ma_pb
        ,ma_gd,ma_tb,dich_vu,to_char(ngay_bbbg,'dd/mm/yyyy') ngay_sd,thang_ptm,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
       ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro,heso_diaban_tinhkhac, tyle_huongdt, heso_hoso
       , heso_hotro_nvptm, heso_hotro_nvhotro, manv_tt_dai
       ,thang_tlkpi thang_tlkpi_nv,thang_tlkpi_to,doanhthu_kpi_nvptm dthu_kpi
  from ttkd_bsc.ct_bsc_ptm a
 where thang_tlkpi_to=202407 and loaitb_id=21
 
)m

 
 group by m.ma_nv,m.ma_to,ma_pb,ma_gd,ma_tb,dich_vu, ngay_sd,thang_ptm,dthu_goi,dthu_goi_ngoaimang,tien_dnhm,tien_sodep
       ,heso_dichvu,heso_dichvu_1,heso_dichvu_dnhm,heso_quydinh_nvptm,heso_quydinh_nvhotro,heso_diaban_tinhkhac
       ,tyle_huongdt, heso_hoso, heso_hotro_nvptm, heso_hotro_nvhotro, manv_tt_dai

