---Update heso dichvu PS = 0.5, AM = 0.2 doi cac TH PS Setup system (mysafe) Minh Thao
		update ttkd_bsc.ct_bsc_ptm set HESO_DICHVU = 0.2  --AM = 0.2
																, HESO_VTCV_NVHOTRO = 2.5		---PS = 0.5 /0.2
																, ghi_chu = ghi_chu || decode (ghi_chu, null, null, '; ') || ghi_chu
		where ma_duan_banhang = '214147'
;
---Update DTHU GOI (minh thao)
		update ttkd_bsc.ct_bsc_ptm set DTHU_GOI = 10000000
--			select * from ttkd_bsc.ct_bsc_ptm 
			where ma_duan_banhang = '214147'
;
commit;
rollback;


-- Doanh thu don gia cac dv ngoai tru vnptt, SMS Brandname:
update ttkd_bsc.ct_bsc_ptm a
    set  
		doanhthu_dongia_nvptm = round( dthu_goi *nvl(tyle_huongdt,1) *heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																   *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
																   *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
																   *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(heso_daily,1),0)
            ,
		  doanhthu_dongia_nvhotro = round(dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																   *heso_vtcv_nvhotro * heso_hotro_nvhotro*heso_quydinh_nvhotro*nvl(heso_kvdacthu,1)
																   *nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
																   *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)   ,0)
            ,doanhthu_dongia_dai = round( dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																*heso_quydinh_dai*heso_vtcv_dai*heso_hotro_dai*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
																*nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)  ,0)
            ,doanhthu_kpi_nvptm  = round( dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) ,0)
            ,doanhthu_kpi_to= round( dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) ,0)
            ,doanhthu_kpi_phong= round( dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) ,0)
            ,doanhthu_kpi_nvhotro = (case when manv_hotro is not null 
                                                                            then round(dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_quydinh_nvhotro,1)
                                                                                    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                                    *nvl(heso_kvdacthu,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*tyle_hotro   ,0)
                                                                            end)
            ,doanhthu_kpi_nvdai=(case when manv_tt_dai is not null 
																	then round( dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																				*heso_quydinh_dai*heso_vtcv_dai*heso_hotro_dai*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
																				*nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)  ,0)
																		end
												)
		--	, thang_luong= 35									
--    select thang_luong, thang_ptm, chuquan_id, ma_tb, tenkieu_ld, dich_vu, sothang_dc, dthu_ps, manv_ptm, manv_hotro, dthu_goi, tyle_hotro, heso_dichvu, heso_khachhang, 
--					heso_hotro_nvhotro, heso_quydinh_nvptm,heso_vtcv_nvptm, heso_khuyenkhich, heso_tratruoc, heso_kvdacthu
--					, heso_hoso, heso_quydinh_nvhotro, heso_vtcv_nvhotro
--					, doanhthu_dongia_nvptm, luong_dongia_nvptm, luong_dongia_nvhotro
--					,  round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
--																   *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
--																   *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
--																   *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)*nvl(heso_daily,1),0
--								) dthu_dongia_new
--								, doanhthu_dongia_nvhotro, doanhthu_kpi_nvhotro, doanhthu_kpi_phong
--										, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong
--										, thang_tldg_dt_nvhotro, thang_tlkpi_hotro
--										, lydo_khongtinh_luong, lydo_khongtinh_dongia, ma_duan_banhang
--			from ttkd_bsc.ct_bsc_ptm a
	
    where  --(hdtb_id, thuebao_id) in (select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL where thang = a.thang_ptm)
			(thang_luong in (3, 5, 6)  --and thang_ptm <> 202405
			or thang_luong between 7 and 100)
			--and thang_luong in (86, 87) and manv_hotro is not null
--			and thang_luong in  (24, 25, 26)
			and thang_luong = 99
		--	and thang_luong= 20 and (LUONG_DONGIA_NVHOTRO = 0 or THANG_TLDG_DT_NVHOTRO is null or THANG_TLDG_DT_NVHOTRO = 202405)
--			thang_luong=9999 and heso_dichvu = 0.3
--				and ma_duan_banhang = '235405'
				;
                       rollback;
				   commit;
            
-- Luong don gia cac dv ngoai tru vnptt, SMS Brandname:
update ttkd_bsc.ct_bsc_ptm a 
    set  luong_dongia_nvptm = nvl(doanhthu_dongia_nvptm,0)*dongia/1000
            ,luong_dongia_nvhotro = nvl(doanhthu_dongia_nvhotro,0)*dongia/1000
            ,luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
--     select thang_ptm, ma_tb, manv_hotro, heso_dichvu, doanhthu_dongia_nvptm, luong_dongia_nvptm, luong_dongia_nvhotro
--			, nvl(doanhthu_dongia_nvptm,0)*dongia/1000 new, doanhthu_dongia_dnhm from ttkd_bsc.ct_bsc_ptm a
    where --(hdtb_id, thuebao_id) in (select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL where thang = a.thang_ptm)
					--thang_luong=202404 --and  heso_daily = 0.05
--					ma_tb = 'hcm_ca_00067906'
				(thang_luong in (3, 5, 6)  --and thang_ptm <> 202405
			or thang_luong between 7 and 100000) 
				and thang_luong=99-- and heso_dichvu = 0.3
--	and thang_luong = 26
--			and thang_luong= 21 and (LUONG_DONGIA_NVHOTRO = 0 or THANG_TLDG_DT_NVHOTRO is null or THANG_TLDG_DT_NVHOTRO = 202405)
--					and ma_duan_banhang = '235405'
					;
            
commit;


-- Doanh thu goi tich hop: ap dung khong phan biet tap quan ly (theo VB 275/TTr-NS-DH 22/06/2020)
update ttkd_bsc.ct_bsc_ptm a 
    set doanhthu_dongia_nvptm =
                        (case when goi_id=15599  then round( (dthu_goi *nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.21/0.1434 ,0)  -- SME_NEW
                                    when goi_id=15600  then round( (dthu_goi *nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.25/0.17 ,0)  -- SME+
                                    when goi_id=15602  then round( (dthu_goi *nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.25/0.21 ,0)  -- SME_BASIC 1
                                    when goi_id=15601  then round( (dthu_goi *nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.35/0.30 ,0)  -- SME_BASIC 2   
                                    when goi_id=15604  then round( (dthu_goi *nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.19/0.13 ,0)  -- SME_SMART1
                                    when goi_id=15603  then round( (dthu_goi *nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.20/0.14 ,0)  -- SME_SMART2
                                    when goi_id=15605  then round( (dthu_goi *nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.20/0.16 ,0)  -- F_Pharmacy
                                    when goi_id=15596  then round( (dthu_goi *nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.20/0.15 ,0)  -- F_ORM
                         end)                                                                                                                                                       
    -- select goi_id, ma_tb, dthu_goi, doanhthu_dongia_nvptm from ttkd_bsc.ct_bsc_ptm a
    where (thang_luong in (3, 5, 6)  --and thang_ptm <> 202405
						or thang_luong between 7 and 100) 
			and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605)
    --(hdtb_id, thuebao_id) in (select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL where thang = a.thang_ptm)
				;
                   

            
-- SMS Brandname: Tinh lai dthu don gia dv sms brandname cua thang n-1: 
		update ttkd_bsc.ct_bsc_ptm a  
		    set    doanhthu_dongia_nvptm = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
															*nvl(tyle_huongdt,1)
														    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm
														    *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)
														    *heso_khachhang*nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ,0)
				  ,doanhthu_dongia_nvhotro =null  
				  ,doanhthu_kpi_nvptm         = round(((nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
														  *nvl(tyle_huongdt,1)
														  *nvl(heso_quydinh_nvptm,1)*nvl(heso_diaban_tinhkhac,1)
														  *decode(tyle_hotro,null,1,1-tyle_hotro)*decode(manv_tt_dai,null,1,0.5)  ,0)                                                              
				  ,doanhthu_kpi_nvhotro      = case when manv_hotro is not null 
															   then round(((nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
																		*nvl(tyle_huongdt,1)
																		*nvl(heso_quydinh_nvhotro,1)*tyle_hotro * nvl(heso_diaban_tinhkhac,1) ,0)
														    end
				  ,doanhthu_kpi_phong        = round(((nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
														*nvl(tyle_huongdt,1)
														  *decode(tyle_hotro,null,1,1-tyle_hotro) * decode(manv_tt_dai,null,1,0.5) *nvl(heso_diaban_tinhkhac,1) ,0)                                                              
		    -- select ma_tb, dthu_goi, dthu_goi_ngoaimang, heso_dichvu, heso_dichvu_1, thang_tldg_dt, thang_tlkpi_phong,lydo_khongtinh_luong, doanhthu_dongia_nvptm from ttkd_bsc.ct_bsc_ptm a
		    where (thang_luong in (3, 5, 6)  --and thang_ptm <> 202405
								or thang_luong between 7 and 100) 
						and loaitb_id in (131)
		    ;
                                                
          

update ttkd_bsc.ct_bsc_ptm 
    set  luong_dongia_nvptm=nvl(doanhthu_dongia_nvptm,0)*dongia/1000,
            luong_dongia_nvhotro = nvl(doanhthu_dongia_nvhotro,0)*dongia/1000,    
            luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
    where  (thang_luong in (3, 5, 6)  --and thang_ptm <> 202405
								or thang_luong between 7 and 100) 
				and loaitb_id in (131);
                           
 
-- loaitb_id = 303:
update ttkd_bsc.ct_bsc_ptm a  
    set  doanhthu_dongia_nvptm = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                    *nvl(tyle_huongdt,1)
													   *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(heso_daily,1),0)
            ,doanhthu_dongia_nvhotro = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                    *nvl(tyle_huongdt,1)
													   *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *heso_quydinh_nvhotro*nvl(heso_kvdacthu,1)*heso_vtcv_nvhotro
                                                                    *tyle_hotro*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)  ,0)
            ,doanhthu_dongia_dai = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                     *nvl(tyle_huongdt,1)
													    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1) * heso_quydinh_dai 
                                                                     *heso_vtcv_dai*heso_hotro_dai*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                     *nvl(heso_diaban_tinhkhac,1) ,0)
            ,doanhthu_kpi_nvptm  = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                    *nvl(tyle_huongdt,1)
													   *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) ,0)
            ,doanhthu_kpi_to          = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                    *nvl(tyle_huongdt,1)
													   *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) ,0)
            ,doanhthu_kpi_phong = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                    *nvl(tyle_huongdt,1)
													   *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) ,0)
            ,doanhthu_kpi_nvhotro = (case when manv_hotro is not null 
                                                                            then round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                                    *nvl(tyle_huongdt,1)
																    *nvl(heso_quydinh_nvhotro,1)
                                                                                    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                                    *nvl(heso_kvdacthu,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                                    *nvl(heso_diaban_tinhkhac,1)*tyle_hotro   ,0)
                                                                            end)
            ,doanhthu_kpi_nvdai=(case when manv_tt_dai is not null then doanhthu_dongia_dai end)       
    -- select thang_ptm, chuquan_id, ma_tb, tenkieu_ld, dich_vu, sothang_dc, dthu_ps, dthu_goi, tyle_hotro, heso_dichvu from ttkd_bsc.ct_bsc_ptm
    where (thang_luong in (3, 5, 6)  --and thang_ptm <> 202405
								or thang_luong between 7 and 100) 
								and loaitb_id=303;
    
update ttkd_bsc.ct_bsc_ptm 
    set  luong_dongia_nvptm=nvl(doanhthu_dongia_nvptm,0)*dongia/1000,
            luong_dongia_nvhotro = nvl(doanhthu_dongia_nvhotro,0)*dongia/1000,    
            luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
    where  (thang_luong in (3, 5, 6)  --and thang_ptm <> 202405
								or thang_luong between 7 and 100) 
					and loaitb_id in (303);
    
    
-- dthu kpi cua dich vu giai phap thiet bi, giai phap CNTT: 
    -- dthu tinh bsc la dthu hop dong x cac he so tinh bsc (ko tinh tren chenh lech thu chi). => dthu_goi_goc
update ttkd_bsc.ct_bsc_ptm a
    set doanhthu_dongia_nvptm = round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)*nvl(heso_daily,1),0)
        ,doanhthu_kpi_nvptm = round(dthu_goi_goc*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) *nvl(tyle_huongdt,1) ,0)
        ,doanhthu_kpi_to          = round( dthu_goi_goc*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
        ,doanhthu_kpi_phong  = round( dthu_goi_goc*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
        ,doanhthu_kpi_nvhotro = (case when manv_hotro is not null 
                                                                            then round(dthu_goi_goc*heso_dichvu*nvl(heso_quydinh_nvhotro,1)
                                                                                                *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                                                *nvl(heso_kvdacthu,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                                                *nvl(heso_diaban_tinhkhac,1)*tyle_hotro*nvl(tyle_huongdt,1)   ,0)
                                                        end)
    -- select dich_vu, ma_gd, ma_tb, dthu_goi_goc, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, doanhthu_kpi_nvptm,doanhthu_kpi_phong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a 
    where (thang_luong in (3, 5, 6)  --and thang_ptm <> 202405
								or thang_luong between 7 and 100) 
								and bo_dau(dich_vu) like '%Thiet bi giai phap%' 
								;
                           
    
update ttkd_bsc.ct_bsc_ptm 
    set  luong_dongia_nvptm=nvl(doanhthu_dongia_nvptm,0)*dongia/1000,
            luong_dongia_nvhotro = nvl(doanhthu_dongia_nvhotro,0)*dongia/1000,    
            luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
    where  (thang_luong in (3, 5, 6)  --and thang_ptm <> 202405
								or thang_luong between 7 and 100) 
				and bo_dau(dich_vu) like '%Thiet bi giai phap%' 
				
				;
            
-- kpi goi SME: ko xet heso_quydinh_nvptm vi ko phan biet tap KH
update ttkd_bsc.ct_bsc_ptm a
    set doanhthu_kpi_phong = round(dthu_goi*nvl(heso_dichvu,1)
                                                      *decode(tyle_hotro,null,1,1-tyle_hotro)*decode(manv_tt_dai,null,1,0.5)  ,0)*nvl(heso_tbnganhan,1)
                                                      *nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)
--   select * from ttkd_bsc.ct_bsc_ptm
    where  (thang_luong in (3, 5, 6)  --and thang_ptm <> 202405
								or thang_luong between 7 and 100) 
				and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605);
           