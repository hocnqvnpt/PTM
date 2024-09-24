---viet ham 
		---thang + 1 len *** quet hso nop tre so voi ngay 13 thang n+ 2, ap dung thang_ptm = n-3, n-2
		update ttkd_bsc.ct_bsc_ptm a set  thang_luong='3', heso_hoso=0.8
		    -- select thang_luong, ma_tb, tenkieu_ld, ngay_bbbg, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du, mien_hsgoc, bs_luukho , heso_hoso from ttkd_bsc.ct_bsc_ptm a
		    where ((thang_ptm = 202402 and (to_number(to_char(ngay_luuhs_ttkd,'yyyymmdd'))>20240413 or to_number(to_char(ngay_luuhs_ttvt,'yyyymmdd'))>20240413))
							or (thang_ptm = 202403 and (to_number(to_char(ngay_luuhs_ttkd,'yyyymmdd'))>20240513 or to_number(to_char(ngay_luuhs_ttvt,'yyyymmdd'))>20240513))
--							or (thang_ptm = 202404 and (to_number(to_char(ngay_luuhs_ttkd,'yyyymmdd'))>20240613 or to_number(to_char(ngay_luuhs_ttvt,'yyyymmdd'))>20240613))
						)
					  and (loaitb_id not in (21) or ma_kh='GTGT rieng') 
					  and mien_hsgoc is null and nop_du = 1 and substr(bs_luukho,1,6)= '202406' --and heso_hoso is null		---thang n+1
					  and (thang_tldg_dt = 202405 or thang_tldg_dt is null) 
--					  and thang_luong<>'3'
			  ;
     ---1
	      --      select thang_luong, ma_tb, tenkieu_ld, ngay_bbbg, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du, mien_hsgoc, bs_luukho , heso_hoso from ttkd_bsc.ct_bsc_ptm a
		--    where substr(bs_luukho,1,6)='202403';
-- Doanh thu don gia cac dv ngoai tru vnptt, SMS Brandname:
			update ttkd_bsc.ct_bsc_ptm a 
			    set   doanhthu_dongia_nvptm = round(dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																		 *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
																		 *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
																		 *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)*nvl(heso_daily,1) ,0)
					  ,doanhthu_dongia_dai       = case when manv_tt_dai is not null 
																	  then round(dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																			*heso_quydinh_dai*heso_vtcv_dai*heso_hotro_dai*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
																			*nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ,0)
															end
					  ,doanhthu_kpi_nvptm = round(dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																				   *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
																				   *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
																				   *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)*nvl(heso_daily,1) ,0)
					  ,doanhthu_kpi_nvdai = case when manv_tt_dai is not null 
																	  then round(dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																			*heso_quydinh_dai*heso_vtcv_dai*heso_hotro_dai*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
																			*nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ,0)
															end
					  ,doanhthu_kpi_to= round(dthu_goi*nvl(heso_dichvu,1)*nvl(heso_dichvu_1,1)
												   *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
												   *decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm)
												   *nvl(heso_hoso,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
												   *nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ,0)
					  ,doanhthu_kpi_phong= round(dthu_goi*nvl(heso_dichvu,1)*nvl(heso_dichvu_1,1)
												   *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
												   *decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm)
												   *nvl(heso_hoso,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
												   *nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ,0)
		---	    select * from ttkd_bsc.ct_bsc_ptm a 
			    where  thang_luong='3' 
			    ;
	--2		    
    
        
                        
            
-- Doanh thu goi tich hop: ap dung khong phan biet tap quan ly (theo VB 275/TTr-NS-DH 22/06/2020)
        -- tham khao bang huong dan cua anh Nghia tinh he so cac goi tich hop cho SMEs: VB 275/TTKD HCM-DH 22/06/2020:
		update ttkd_bsc.ct_bsc_ptm a 
		    set doanhthu_dongia_nvptm =
						    (case when goi_id=15599  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																			   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_hoso,1)
																			   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.21/0.1434 ,0)  -- SME_NEW
									 when goi_id=15600  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																			   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_hoso,1)
																			   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.25/0.17 ,0)  -- SME+
									 when goi_id=15602  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																			   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_hoso,1)
																			   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.25/0.21 ,0)  -- SME_BASIC 1
									 when goi_id=15601  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																			   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_hoso,1)
																			   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.35/0.30 ,0)  -- SME_BASIC 2   
									 when goi_id=15604  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																			   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_hoso,1)
																			   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.19/0.13 ,0)  -- SME_SMART1
									 when goi_id=15603  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																			   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_hoso,1)
																			   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.20/0.14 ,0)  -- SME_SMART2
									 when goi_id=15605  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																			   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_hoso,1)
																			   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.20/0.16 ,0)  -- F_Pharmacy
									 when goi_id=15596  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																			   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_hoso,1)
																			   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.20/0.15 ,0)  -- F_ORM
							end)                                                                                                                                                       
		    -- select ma_tb, dthu_goi, doanhthu_dongia_nvptm, dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_hoso,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1) dthu_dongia_new from ttkd_bsc.ct_bsc_ptm
		    where  thang_luong='3' and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605)
		    ;
		    ---3
		                                              
                            
-- Luong don gia cac dv ngoai tru vnptt, SMS Brandname:
		update ttkd_bsc.ct_bsc_ptm a 
		    set  luong_dongia_nvptm=nvl(doanhthu_dongia_nvptm,0)*dongia/1000,
				  luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
		    where  thang_luong='3'
		    ;
                       ---4
            
-- SMS Brandname: Tinh lai dthu don gia dv sms brandname cua thang n-1: 
		update ttkd_bsc.ct_bsc_ptm a 
		    set  doanhthu_dongia_nvptm = round(((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1))
														*nvl(tyle_huongdt,1)
														*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm
														*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_hoso,1)
														*heso_khachhang*nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1),0)
		    -- select thang_ptm, ma_tb, dthu_goi, dthu_goi_ngoaimang, heso_dichvu, heso_dichvu_1, thang_tldg_dt, thang_tlkpi_phong,lydo_khongtinh_luong from ttkd_bsc.ct_bsc_ptm 
		    where thang_luong='3' and loaitb_id=131
		    ;
		    ---5
                       
            
		update ttkd_bsc.ct_bsc_ptm 
		    set luong_dongia_nvptm=nvl(doanhthu_dongia_nvptm,0)*dongia/1000,
				luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
		    where thang_luong='3' and loaitb_id=131
		    ;
			---6

		update ttkd_bsc.ct_bsc_ptm a
		    set   doanhthu_kpi_nvptm=doanhthu_dongia_nvptm
				  ,doanhthu_kpi_to= round(((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1))
											   *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
											   *decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm)
											   *nvl(heso_hoso,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
											   *nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ,0)
				  ,doanhthu_kpi_phong= round(((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1))
											   *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
											   *decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm)
											   *nvl(heso_hoso,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
											   *nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ,0)
		    where thang_luong='3' and loaitb_id=131
		    ;
              ---7       
		    commit;