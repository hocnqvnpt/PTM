-- nv ptm:
drop view ttkd_bsc.v_ct_bsc_ptm_202405;
create OR REPLACE VIEW ttkd_bsc.v_ct_bsc_ptm_202405 as
 select thang_ptm,ma_gd, ma_gd_gt, tenkieu_ld,ma_tb,dich_vu,ten_tb,diachi_ld--,sdt_lh, email_lh
				,sothang_dc,ma_da,
				   to_char(ngay_bbbg,'dd/mm/yyyy') ngay_bbbg, to_char(ngay_luuhs_ttkd,'dd/mm/yyyy') ngay_luuhs_ttkd, to_char(ngay_luuhs_ttvt,'dd/mm/yyyy') ngay_luuhs_ttvt,
				   to_char(ngay_scan,'dd/mm/yyyy') ngay_scan,goi_cuoc--,duq_cap1_dkgoi,goi_ckdai,dthu_kit_tratruoc,dthu_nt_thangkh,
			--	   dthu_tkc_thangkh,tinh_pstkc_thangkh, kenh_noibo, thoadk_bsc, thoadk_dg, dongia_bh, dongia_kk,
--				   , (case when dichvuvt_id=2 then (select ten_dt from ccs_common.doituongs@vinadata where doituong_id=a.doituong_id)
--						  else (select ten_dt from css_hcm.doituong where doituong_id=a.doituong_id) end) doituong_tb,
--				   (case when dichvuvt_id=2 then (select ten from ccs_common.trangthai_tbs@vinadata where trangthai_id=a.trangthaitb_id)
--						  else (select trangthai_tb from css_hcm.trangthai_tb where trangthaitb_id=a.trangthaitb_id) end) trangthaitb_n,
--				   (case when dichvuvt_id=2 then (select ten from ccs_common.trangthai_tbs@vinadata where trangthai_id=a.trangthaitb_id_n1)
--						  else (select trangthai_tb from css_hcm.trangthai_tb where trangthaitb_id=a.trangthaitb_id_n1) end) trangthaitb_n1,                 
--				   (case when dichvuvt_id=2 then (select ten from ccs_common.trangthai_tbs@vinadata where trangthai_id=a.trangthaitb_id_n2)
--						  else (select trangthai_tb from css_hcm.trangthai_tb where trangthaitb_id=a.trangthaitb_id_n2) end) trangthaitb_n2, 
--				   (case when dichvuvt_id=2 then (select ten from ccs_common.trangthai_tbs@vinadata where trangthai_id=a.trangthaitb_id_n3)
--						  else (select trangthai_tb from css_hcm.trangthai_tb where trangthaitb_id=a.trangthaitb_id_n3) end) trangthaitb_n3,         
				   , nocuoc_ptm,nocuoc_n1,nocuoc_n2,nocuoc_n3, 
				   ma_tiepthi,ma_nguoigt,nguoi_gt,manv_tt_dai,ma_to_dai,manv_hotro,tyle_hotro,
				   
				   manv_ptm ma_nv,ten_pb,ma_to,ten_to,ma_pb, ghi_chu,lydo_khongtinh_luong
				--   (select tenphong from ttkd_bct.phongbanhang where pbh_id=a.pbh_ql_id) pbh_ql
				   ,tien_dnhm,tien_sodep,ngay_tt,soseri,tien_tt,
				   dthu_ps_truoc,dthu_ps,dthu_ps_n1,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,chiphi_ttkd,
				   doituong_kh,khhh_khm,diaban, kiemtra_duan, ma_duan_banhang, id_447, phanloai_kh,heso_khachhang,heso_dichvu,heso_dichvu_1,heso_tratruoc,heso_khuyenkhich,
				   heso_tbnganhan,heso_kvdacthu,heso_vtcv_nvptm,
				   heso_vtcv_dai,heso_vtcv_nvhotro,heso_hotro_nvptm,heso_hotro_dai,heso_hotro_nvhotro,
				   heso_quydinh_nvptm,heso_quydinh_dai,heso_quydinh_nvhotro,heso_diaban_tinhkhac,tyle_huongdt, heso_hoso,
				   doanhthu_dongia_nvptm,doanhthu_dongia_dai,doanhthu_dongia_nvhotro,
				   heso_dichvu_dnhm,doanhthu_dongia_dnhm,doanhthu_kpi_nvptm,
				   thang_tldg_dnhm,thang_tldg_dt,thang_tlkpi,thang_tlkpi_to,lydo_khongtinh_dongia,
				   luong_dongia_dnhm_nvptm,luong_dongia_nvptm,luong_dongia_dai,luong_dongia_nvhotro, thang_tldg_dt_nvhotro,
				   doanhthu_kpi_dnhm, thang_tlkpi_dnhm
from ttkd_bsc.ct_bsc_ptm a
 where (loaitb_id not in (20,21) or (loaitb_id=20 and goi_luongtinh is null) or loaitb_id is null)
              and   (thang_tldg_dt=202405 or thang_tlkpi=202405 or thang_tlkpi_dnhm=202405
                              or (thang_ptm between 202402 and 202405 and thang_tldg_dt is null) 
                              )
  ;          
union all  -- VNPTT
;
 select thang_ptm, tenkieu_ld,ma_tb,dich_vu, datcoc_csd, thang_bddc, thang_ktdc, sothang_dc,
        to_char(ngay_bbbg,'dd/mm/yyyy') ngay_bbbg
        ,goi_cuoc , manv_ptm ma_nv,ten_pb,ma_to,ten_to,ma_pb, ghi_chu
       ,dthu_goi_goc,dthu_goi
        , doanhthu_dongia_nvptm, doanhthu_dongia_nvhotro
        , heso_dichvu_dnhm,doanhthu_dongia_dnhm,doanhthu_kpi_nvptm,
        thang_tldg_dnhm,thang_tldg_dt,thang_tlkpi,thang_tlkpi_to,lydo_khongtinh_dongia,
        luong_dongia_dnhm_nvptm,luong_dongia_nvptm,luong_dongia_nvhotro,thang_tldg_dt_nvhotro,
        doanhthu_kpi_dnhm, thang_tlkpi_dnhm
 from ttkd_bsc.ct_bsc_ptm a
 where loaitb_id=21 and thang_ptm=202405
 ;
                    

 
-- nv Dai/PKTTT (VNPTS)
 drop view ttkd_bsc.v_ct_bsc_ptm_dai_202405;
 create OR REPLACE VIEW ttkd_bsc.v_ct_bsc_ptm_dai_202405 as
 select 
 thang_ptm,ma_gd,ma_gd_gt,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,sothang_dc,ma_da,
        to_char(ngay_bbbg,'dd/mm/yyyy') ngay_bbbg, to_char(ngay_luuhs_ttkd,'dd/mm/yyyy') ngay_luuhs_ttkd, to_char(ngay_luuhs_ttvt,'dd/mm/yyyy') ngay_luuhs_ttvt,
        to_char(ngay_scan,'dd/mm/yyyy') ngay_scan,goi_cuoc,tinh_xuathang,pheduyet_ok,duq_cap1_dkgoi,goi_ckdai,dthu_kit_tratruoc,dthu_nt_thangkh,
        dthu_tkc_thangkh,tinh_pstkc_thangkh,THOADK_BSC, THOADK_DG, DONGIA_BH, DONGIA_KK,
--        (case when dichvuvt_id=2 then (select ten_dt from ccs_common.doituongs@vinadata where doituong_id=a.doituong_id)
--                 else (select ten_dt from css_hcm.doituong where doituong_id=a.doituong_id) end) doituong_tb,
--        (case when dichvuvt_id=2 then (select ten from ccs_common.trangthai_tbs@vinadata where trangthai_id=a.trangthaitb_id)
--                 else (select trangthai_tb from css_hcm.trangthai_tb where trangthaitb_id=a.trangthaitb_id) end) trangthaitb_n,
--        (case when dichvuvt_id=2 then (select ten from ccs_common.trangthai_tbs@vinadata where trangthai_id=a.trangthaitb_id_n1)
--                 else (select trangthai_tb from css_hcm.trangthai_tb where trangthaitb_id=a.trangthaitb_id_n1) end) trangthaitb_n1,                 
--        (case when dichvuvt_id=2 then (select ten from ccs_common.trangthai_tbs@vinadata where trangthai_id=a.trangthaitb_id_n2)
--                 else (select trangthai_tb from css_hcm.trangthai_tb where trangthaitb_id=a.trangthaitb_id_n2) end) trangthaitb_n2, 
--        (case when dichvuvt_id=2 then (select ten from ccs_common.trangthai_tbs@vinadata where trangthai_id=a.trangthaitb_id_n3)
--                 else (select trangthai_tb from css_hcm.trangthai_tb where trangthaitb_id=a.trangthaitb_id_n3) end) trangthaitb_n3,         
        nocuoc_ptm,nocuoc_n1,nocuoc_n2,nocuoc_n3,                 
        ma_tiepthi,ma_nguoigt,nguoi_gt,manv_ptm,ma_to ma_to_ptm,manv_hotro,tyle_hotro,manv_tt_dai ma_nv,  -- ma_to_dai
        (select ma_to from ttkd_bsc.nhanvien where thang = 202405 and  manv_hrm=a.manv_tt_dai) ma_to,
        (select ten_to from ttkd_bsc.nhanvien where thang = 202405 and  manv_hrm=a.manv_tt_dai) ten_to,
        (select ma_pb from ttkd_bsc.nhanvien where thang = 202405 and  manv_hrm=a.manv_tt_dai) ma_pb,
        (select ma_pb from ttkd_bsc.nhanvien where thang = 202405 and  manv_hrm=a.manv_tt_dai) ten_pb,
        ghi_chu,lydo_khongtinh_luong,
        kh_id
--	   ,(select tenphong from ttkd_bct.phongbanhang where pbh_id=a.pbh_ql_id) pbh_ql
	   ,tien_dnhm,tien_sodep,ngay_tt,soseri,tien_tt,
        dthu_ps_truoc,dthu_ps,dthu_ps_n1,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,
        doituong_kh,khhh_khm,diaban,phanloai_kh, heso_khachhang,heso_dichvu,heso_dichvu_1,heso_tratruoc,heso_khuyenkhich,
        heso_tbnganhan,heso_kvdacthu,heso_vtcv_nvptm,
        heso_vtcv_dai,heso_vtcv_nvhotro,heso_hotro_nvptm,heso_hotro_dai,heso_hotro_nvhotro,
        heso_quydinh_nvptm,heso_quydinh_dai,heso_quydinh_nvhotro,heso_diaban_tinhkhac,heso_hoso,
        doanhthu_dongia_nvptm,doanhthu_dongia_dai,doanhthu_dongia_nvhotro,
        heso_dichvu_dnhm,doanhthu_dongia_dnhm,doanhthu_kpi_nvdai,
        thang_tldg_dnhm,thang_tldg_dt,thang_tldg_dt_dai,thang_tlkpi,thang_tlkpi_to,lydo_khongtinh_dongia,
        luong_dongia_dnhm_nvptm,luong_dongia_nvptm,luong_dongia_dai,luong_dongia_nvhotro
 from ttkd_bsc.ct_bsc_ptm a
 where  thang_ptm between 202402 and 202405 
                and (thang_tldg_dt=202405 or thang_tldg_dt is null or thang_tldg_dt_dai=202405 )
                and manv_tt_dai is not null;

  -- Ho tro cua Phong Giai phap:
 drop view ttkd_bsc.v_ct_bsc_ptm_phongGP_202405;
 create OR REPLACE VIEW ttkd_bsc.v_ct_bsc_ptm_phongGP_202405 as
 select 
        thang_ptm, ma_duan, ma_gd,ma_tb,dich_vu,ten_tb,diachi_ld,
        to_char(ngay_bbbg,'dd/mm/yyyy') ngay_bbbg, 
--        (case when dichvuvt_id=2 then (select ten from ccs_common.trangthai_tbs@vinadata where trangthai_id=a.trangthaitb_id)
--                 else (select trangthai_tb from css_hcm.trangthai_tb where trangthaitb_id=a.trangthaitb_id) end) trangthaitb_n,
--       (case when dichvuvt_id=2 then (select ten from ccs_common.trangthai_tbs@vinadata where trangthai_id=a.trangthaitb_id_n1)
--                 else (select trangthai_tb from css_hcm.trangthai_tb where trangthaitb_id=a.trangthaitb_id_n1) end) trangthaitb_n1,          
        ma_tiepthi,ma_nguoigt,nguoi_gt,manv_ptm,ma_to ma_to_ptm,manv_hotro,tyle_hotro, tyle_hotro_nv, manv_hotro ma_nv,
        (select ma_to from ttkd_bsc.nhanvien where thang = 202405 and manv_hrm=a.manv_hotro) ma_to,
        (select ten_to from ttkd_bsc.nhanvien where thang = 202405 and  manv_hrm=a.manv_hotro) ten_to,
        (select ma_pb from ttkd_bsc.nhanvien where thang = 202405 and  manv_hrm=a.manv_hotro) ma_pb,
        (select ma_pb from ttkd_bsc.nhanvien where thang = 202405 and  manv_hrm=a.manv_hotro) ten_pb,
        lydo_khongtinh_luong,dthu_ps,dthu_goi,dthu_goi_ngoaimang,
        doituong_kh,khhh_khm,diaban,phanloai_kh, heso_khachhang,heso_dichvu,heso_dichvu_1,heso_tratruoc,heso_khuyenkhich,
        heso_tbnganhan,heso_kvdacthu,heso_vtcv_nvptm,tyle_huongdt, 
        heso_vtcv_nvhotro,heso_hotro_nvptm,heso_hotro_nvhotro,heso_quydinh_nvhotro, heso_diaban_tinhkhac,
        doanhthu_kpi_nvptm,doanhthu_kpi_nvhotro,
        doanhthu_dongia_nvhotro, luong_dongia_nvhotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia
 from ttkd_bsc.ct_bsc_ptm_pgp a
 where (thang_ptm=202405 
              or ((thang_ptm between 202402 and 202405) and (thang_tldg_dt_nvhotro is null or thang_tldg_dt_nvhotro=202405 ))
              or thang_tlkpi_hotro=202405) 
              and exists (select 1 from ttkd_bsc.nhanvien where thang = 202405 and  ma_pb='VNP0702600' and ma_nv=manv_hotro);
              
 --view chi tieu 044
 -- nv ptm:
drop view ttkd_bsc.v_ct_bsc_cntt_202405;

select * from ttkd_bsc.v_ct_bsc_cntt_202405
;
create OR REPLACE VIEW ttkd_bsc.v_ct_bsc_cntt_202405 as
			 select thang_ptm,ma_gd, ma_gd_gt, tenkieu_ld,ma_tb,dich_vu,ten_tb,diachi_ld--,sdt_lh, email_lh
							,sothang_dc,ma_da,
							   to_char(ngay_bbbg,'dd/mm/yyyy') ngay_bbbg, to_char(ngay_luuhs_ttkd,'dd/mm/yyyy') ngay_luuhs_ttkd, to_char(ngay_luuhs_ttvt,'dd/mm/yyyy') ngay_luuhs_ttvt,
							   to_char(ngay_scan,'dd/mm/yyyy') ngay_scan,goi_cuoc
							   ma_tiepthi,ma_nguoigt,nguoi_gt,manv_tt_dai,ma_to_dai,manv_hotro,tyle_hotro,
							   manv_ptm ma_nv,ten_pb,ma_to,ten_to,ma_pb, ghi_chu,lydo_khongtinh_luong
							   ,tien_dnhm,tien_sodep,ngay_tt,soseri,tien_tt,
							   dthu_ps_truoc,dthu_ps,dthu_ps_n1,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang
							   , doituong_kh,khhh_khm,diaban,phanloai_kh,heso_khachhang,heso_dichvu,heso_dichvu_1,heso_tratruoc,heso_khuyenkhich,
							   heso_tbnganhan,heso_kvdacthu,heso_vtcv_nvptm,
							   heso_vtcv_dai,heso_vtcv_nvhotro,heso_hotro_nvptm,heso_hotro_dai,heso_hotro_nvhotro,
							   heso_quydinh_nvptm,heso_quydinh_dai,heso_quydinh_nvhotro,heso_diaban_tinhkhac,tyle_huongdt, heso_hoso,
							   heso_dichvu_dnhm
							   ,lydo_khongtinh_dongia
							   , doanhthu_kpi_nvptm, doanhthu_kpi_dnhm
							   , doanhthu_kpi_to, doanhthu_kpi_phong, doanhthu_kpi_dnhm_phong
							  , thang_tlkpi_dnhm_to, thang_tlkpi_to, thang_tlkpi_dnhm, thang_tlkpi
			from ttkd_bsc.ct_bsc_ptm a
			 where thang_tlkpi=202405
											  and (exists(select * from ttkd_bsc.dm_loaihinh_hsqd 
																		where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
															or loaitb_id = 999
													)
  ;          

 -- View Tong hop bang luong don gia ptm:
drop view ttkd_bsc.v_bangluong_dongia_ptm_202405;
create OR REPLACE VIEW ttkd_bsc.v_bangluong_dongia_ptm_202405 as
select 
		ma_nv,ten_nv,ma_vtcv,ten_vtcv, ma_pb, ten_pb, ma_to,ten_to,dtptm_dongia_cdbr,dtptm_dongia_cntt,
            dtptm_dongia_vnpts, dtptm_quydinh, dtptm_muctieu, heso_qd_tong, 
            luong_dongia_cdbr,luong_dongia_cntt,luong_dongia_vnpts, luong_dongia_vnptt,ctvxhh_qly_ptr_ctv,
             luong_dongia_ghtt, luong_dongia_nghiepvu, GHTT_VNPTS--, dongluc_ghtt--,dongluc_ghts
		   ,luong_khac, tong_luong_dongia,
            luong_dongia_ptm_thuhoi, thuhoi_dongia_ghtt, luong_dongia_khac_thuhoi,
            giamtru_hosotainha, giamtru_phathuy_qldb, giamtru_ghtt_cntt, tong_luong_thuhoi
from ttkd_bsc.bangluong_dongia_202405;

