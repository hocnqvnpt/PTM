-- Doanh thu don gia cac dv ngoai tru vnptt, SMS Brandname:
update ttkd_bsc.ct_bsc_ptm a
    set  doanhthu_dongia_nvptm = round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																   *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
																   *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
																   *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)*nvl(heso_daily,1),0)
            ,doanhthu_dongia_nvhotro = round(dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *heso_quydinh_nvhotro*nvl(heso_kvdacthu,1)*heso_vtcv_nvhotro
                                                                    *tyle_hotro*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)   ,0)
            ,doanhthu_dongia_dai = round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																*heso_quydinh_dai*heso_vtcv_dai*heso_hotro_dai*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
																*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ,0)
            ,doanhthu_kpi_nvptm  = round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) *nvl(tyle_huongdt,1),0)
            ,doanhthu_kpi_to= round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
            ,doanhthu_kpi_phong= round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
            ,doanhthu_kpi_nvhotro = (case when manv_hotro is not null 
                                                                            then round(dthu_goi*heso_dichvu*nvl(heso_quydinh_nvhotro,1)
                                                                                    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                                    *nvl(heso_kvdacthu,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                                    *nvl(heso_diaban_tinhkhac,1)*tyle_hotro*nvl(tyle_huongdt,1)   ,0)
                                                                            end)
            ,doanhthu_kpi_nvdai=(case when manv_tt_dai is not null then doanhthu_dongia_dai end)     
    /* 
    
    select thang_ptm, chuquan_id, ma_tb, tenkieu_ld, dich_vu, sothang_dc, dthu_ps, dthu_goi, tyle_hotro, heso_dichvu, heso_hotro_nvhotro
					, heso_quydinh_nvptm,heso_vtcv_nvptm, heso_khuyenkhich, heso_tratruoc, heso_kvdacthu, doanhthu_dongia_nvptm, luong_dongia_nvptm
					,  round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																   *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
																   *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
																   *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)*nvl(heso_daily,1),0
								) dthu_dongia_new
								, doanhthu_dongia_nvhotro, doanhthu_kpi_nvhotro
										, thang_tldg_dt
			from ttkd_bsc.ct_bsc_ptm a
	*/
    where  --(hdtb_id, thuebao_id) in (select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL where thang = a.thang_ptm)
			
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
				
					;
            
commit;


-- Doanh thu goi tich hop: ap dung khong phan biet tap quan ly (theo VB 275/TTr-NS-DH 22/06/2020)
update ttkd_bsc.ct_bsc_ptm a 
    set doanhthu_dongia_nvptm =
                        (case when goi_id=15599  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.21/0.1434 ,0)  -- SME_NEW
                                    when goi_id=15600  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.25/0.17 ,0)  -- SME+
                                    when goi_id=15602  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.25/0.21 ,0)  -- SME_BASIC 1
                                    when goi_id=15601  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.35/0.30 ,0)  -- SME_BASIC 2   
                                    when goi_id=15604  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.19/0.13 ,0)  -- SME_SMART1
                                    when goi_id=15603  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.20/0.14 ,0)  -- SME_SMART2
                                    when goi_id=15605  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.20/0.16 ,0)  -- F_Pharmacy
                                    when goi_id=15596  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.20/0.15 ,0)  -- F_ORM
                         end)                                                                                                                                                       
    -- select goi_id, ma_tb, dthu_goi, doanhthu_dongia_nvptm, dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1) dthu_dongia_new from ttkd_bsc.ct_bsc_ptm a
    where thang_luong=18 and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605)
    --(hdtb_id, thuebao_id) in (select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL where thang = a.thang_ptm)
				;
                   

            
-- SMS Brandname: Tinh lai dthu don gia dv sms brandname cua thang n-1: 
		update ttkd_bsc.ct_bsc_ptm a  
		    set    doanhthu_dongia_nvptm = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
														    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm
														    *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)
														    *heso_khachhang*nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ,0)
				  ,doanhthu_dongia_nvhotro =null  
				  ,doanhthu_kpi_nvptm         = round(((nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
														  *nvl(heso_quydinh_nvptm,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)
														  *decode(tyle_hotro,null,1,1-tyle_hotro)*decode(manv_tt_dai,null,1,0.5)  ,0)                                                              
				  ,doanhthu_kpi_nvhotro      = case when manv_hotro is not null 
															   then round(((nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
																		*nvl(heso_quydinh_nvhotro,1)*tyle_hotro * nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
														    end
				  ,doanhthu_kpi_phong        = round(((nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
														  *decode(tyle_hotro,null,1,1-tyle_hotro) * decode(manv_tt_dai,null,1,0.5) *nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)                                                              
		    -- select ma_tb, dthu_goi, dthu_goi_ngoaimang, heso_dichvu, heso_dichvu_1, thang_tldg_dt, thang_tlkpi_phong,lydo_khongtinh_luong from ttkd_bsc.ct_bsc_ptm a
		    where thang_luong=18 and loaitb_id in (131)
		    ;
                                                
          

update ttkd_bsc.ct_bsc_ptm 
    set  luong_dongia_nvptm=nvl(doanhthu_dongia_nvptm,0)*dongia/1000,
            luong_dongia_nvhotro = nvl(doanhthu_dongia_nvhotro,0)*dongia/1000,    
            luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
    where  thang_luong=18 and loaitb_id in (131);
                           
 
-- loaitb_id = 303:
update ttkd_bsc.ct_bsc_ptm a  
    set  doanhthu_dongia_nvptm = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)*nvl(heso_daily,1),0)
            ,doanhthu_dongia_nvhotro = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *heso_quydinh_nvhotro*nvl(heso_kvdacthu,1)*heso_vtcv_nvhotro
                                                                    *tyle_hotro*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)   ,0)
            ,doanhthu_dongia_dai = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                     *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1) * heso_quydinh_dai 
                                                                     *heso_vtcv_dai*heso_hotro_dai*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                     *nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ,0)
            ,doanhthu_kpi_nvptm  = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) *nvl(tyle_huongdt,1),0)
            ,doanhthu_kpi_to          = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
            ,doanhthu_kpi_phong = round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                    *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                    *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                    *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
            ,doanhthu_kpi_nvhotro = (case when manv_hotro is not null 
                                                                            then round( ((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                                    *nvl(heso_quydinh_nvhotro,1)
                                                                                    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                                    *nvl(heso_kvdacthu,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                                    *nvl(heso_diaban_tinhkhac,1)*tyle_hotro*nvl(tyle_huongdt,1)   ,0)
                                                                            end)
            ,doanhthu_kpi_nvdai=(case when manv_tt_dai is not null then doanhthu_dongia_dai end)       
    -- select thang_ptm, chuquan_id, ma_tb, tenkieu_ld, dich_vu, sothang_dc, dthu_ps, dthu_goi, tyle_hotro, heso_dichvu, heso_hotro_nvhotro, heso_quydinh_nvptm,heso_vtcv_nvptm, heso_khuyenkhich, heso_tratruoc, heso_kvdacthu, doanhthu_dongia_nvptm, luong_dongia_nvptm,  round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)*nvl(tyle_huongdt,1) ,0) dthu_dongia_new, thang_tldg_dt from ct_bsc_ptm
    where thang_luong=18 and loaitb_id=303;
    
update ttkd_bsc.ct_bsc_ptm 
    set  luong_dongia_nvptm=nvl(doanhthu_dongia_nvptm,0)*dongia/1000,
            luong_dongia_nvhotro = nvl(doanhthu_dongia_nvhotro,0)*dongia/1000,    
            luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
    where  thang_luong=18 and loaitb_id in (303);
    
    
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
    -- select dich_vu, ma_gd, ma_tb, dthu_goi_goc, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, doanhthu_kpi_nvptm,doanhthu_kpi_phong, lydo_khongtinh_dongia from ct_bsc_ptm a 
    where thang_luong=18 and bo_dau(dich_vu) like '%Thiet bi giai phap%' ;
                           
    
update ttkd_bsc.ct_bsc_ptm 
    set  luong_dongia_nvptm=nvl(doanhthu_dongia_nvptm,0)*dongia/1000,
            luong_dongia_nvhotro = nvl(doanhthu_dongia_nvhotro,0)*dongia/1000,    
            luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
    where  thang_luong=18 and bo_dau(dich_vu) like '%Thiet bi giai phap%' ;
            
-- kpi goi SME: ko xet heso_quydinh_nvptm vi ko phan biet tap KH
update ttkd_bsc.ct_bsc_ptm a
    set doanhthu_kpi_phong = round(dthu_goi*nvl(heso_dichvu,1)
                                                      *decode(tyle_hotro,null,1,1-tyle_hotro)*decode(manv_tt_dai,null,1,0.5)  ,0)*nvl(heso_tbnganhan,1)
                                                      *nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)
    where  thang_luong=18 and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605);
           


    -- PGP: neu nvien ho tro is not null
		--khong phai chay neu khong can
		delete from ttkd_bsc.ct_bsc_ptm_pgp a 
		    where exists(select 1 from ttkd_bsc.ct_bsc_ptm where thang_luong=18 and id  = a.ptm_id)
		    ;
		    select * from ttkd_bsc.ct_bsc_ptm_pgp;
    
insert into ttkd_bsc.ct_bsc_ptm_pgp a
					  (ptm_id, thang_ptm, nguon, ma_duan, ma_gd, ma_tb, dich_vu, dichvuvt_id, loaitb_id, tenkieu_ld, kieuld_id, khachhang_id, thanhtoan_id, thuebao_id, 
					   hdkh_id, hdtb_id, loaihd_id, ten_tb,diachi_ld, so_gt, mst, mst_tt, ma_duan_banhang, ngay_bbbg,
					   trangthaitb_id, trangthaitb_id_n1, trangthaitb_id_n2, trangthaitb_id_n3, nocuoc_ptm, nocuoc_n1, nocuoc_n2, nocuoc_n3,
					   ma_tiepthi, ma_nguoigt, nguoi_gt,manv_ptm, tennv_ptm, ma_to, manv_hotro, tyle_hotro, tyle_hotro_nv,  
					   ghi_chu, lydo_khongtinh_luong, dthu_ps, dthu_goi, dthu_goi_ngoaimang, doituong_kh, khhh_khm,
					   diaban, phanloai_kh, heso_khachhang, heso_dichvu, heso_dichvu_1, heso_tratruoc, heso_khuyenkhich,
					   heso_tbnganhan, heso_kvdacthu, heso_vtcv_nvptm, tyle_huongdt, 
					   heso_vtcv_nvhotro, heso_hotro_nvptm, heso_hotro_nvhotro,heso_quydinh_nvhotro, heso_diaban_tinhkhac, doanhthu_kpi_nvptm, 
					   doanhthu_kpi_nvhotro, doanhthu_dongia_nvhotro, luong_dongia_nvhotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia,
					   dt_dongia_pgp, lg_dongia_pgp, dt_kpi_pgp );
								select id, thang_ptm, nguon, ma_duan_banhang ma_duan, ma_gd, ma_tb, dich_vu, dichvuvt_id, loaitb_id, tenkieu_ld, kieuld_id,  
									   khachhang_id, thanhtoan_id, thuebao_id, hdkh_id, hdtb_id, loaihd_id, ten_tb,diachi_ld, so_gt, mst, mst_tt, ma_duan_banhang, ngay_bbbg,
									   trangthaitb_id, trangthaitb_id_n1, trangthaitb_id_n2, trangthaitb_id_n3, nocuoc_ptm, nocuoc_n1, nocuoc_n2, nocuoc_n3,
									   ma_tiepthi, ma_nguoigt, nguoi_gt,manv_ptm, tennv_ptm, ma_to,
									--   manv_hotro, tyle_hotro, 1 tyle_hotro_nv,  
									   b.manv_presale_hrm manv_hotro, a.tyle_hotro, case when b.tyle_nhom>0 then round(b.tyle_nhom/(a.tyle_hotro*100),2) end tyle_hotro_nv,
									   ghi_chu, lydo_khongtinh_luong, dthu_ps, dthu_goi, dthu_goi_ngoaimang, doituong_kh, khhh_khm,
									   diaban, phanloai_kh, heso_khachhang, heso_dichvu, heso_dichvu_1, heso_tratruoc, heso_khuyenkhich,
									   heso_tbnganhan, heso_kvdacthu, heso_vtcv_nvptm, tyle_huongdt, 
									   heso_vtcv_nvhotro, heso_hotro_nvptm, heso_hotro_nvhotro,heso_quydinh_nvhotro, heso_diaban_tinhkhac, doanhthu_kpi_nvptm, 
									   doanhthu_kpi_nvhotro * 1 doanhthu_kpi_nvhotro, doanhthu_dongia_nvhotro * 1 doanhthu_dongia_nvhotro , 
									   luong_dongia_nvhotro * 1 luong_dongia_nvhotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia,
									   doanhthu_dongia_nvhotro dt_dongia_pgp, luong_dongia_nvhotro lg_dongia_pgp, doanhthu_kpi_nvhotro dt_kpi_gp
								
								    from ttkd_bsc.ct_bsc_ptm a
										    left join (select distinct b.ma_yeucau, c.id_ycdv, b.manv_presale_hrm, b.tyle_nhom, b.tyle, c.ma_dichvu, d.loaitb_id_obss
														  from ttkdhcm_ktnv.amas_booking_presale b, ttkdhcm_ktnv.amas_yeucau_dichvu c,  ttkdhcm_ktnv.amas_loaihinh_tb d
														  where b.id_ycdv = c.id_ycdv and c.ma_dichvu=d.loaitb_id and b.xacnhan=1 
														  ) b on a.ma_duan_banhang=b.ma_yeucau and a.loaitb_id=b.loaitb_id_obss
								    where manv_hotro is not null and (loaitb_id is null or loaitb_id<>21)
									   and thang_luong=86
									   and not exists(select 1 from ttkd_bsc.ct_bsc_ptm_pgp where ptm_id=a.id)
						;

    
update ttkd_bsc.ct_bsc_ptm_pgp a 
    set  (dthu_goi, heso_dichvu, heso_tbnganhan, loaihd_id, tyle_hotro, doanhthu_kpi_nvptm, dt_dongia_pgp,lg_dongia_pgp,dt_kpi_pgp,
             thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia,ghi_chu) 
           = (select dthu_goi, heso_dichvu, heso_tbnganhan, loaihd_id, tyle_hotro, doanhthu_kpi_nvptm, doanhthu_dongia_nvhotro, luong_dongia_nvhotro, doanhthu_kpi_nvhotro,     
                            thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia, ghi_chu
                from ttkd_bsc.ct_bsc_ptm where thang_luong=18 and id=a.ptm_id)
    where exists (select 1 from ttkd_bsc.ct_bsc_ptm where thang_luong=18 and id=a.ptm_id)
    ;
                            
                                        
update ttkd_bsc.ct_bsc_ptm_pgp a 
    set  doanhthu_dongia_nvhotro = round(dt_dongia_pgp * tyle_hotro_nv ,0)
            ,luong_dongia_nvhotro       = round(dt_dongia_pgp * tyle_hotro_nv*0.858,0)
            ,doanhthu_kpi_nvhotro       = round(dt_kpi_pgp * tyle_hotro_nv ,0)
    -- select ptm_id, ma_gd, ma_tb,heso_dichvu, heso_tbnganhan, loaihd_id, dthu_goi, manv_hotro, tyle_hotro_nv, dt_dongia_pgp, lg_dongia_pgp,dt_kpi_pgp,doanhthu_dongia_nvhotro,luong_dongia_nvhotro,doanhthu_kpi_nvhotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro  from ct_bsc_ptm_pgp a
    where exists (select ma_gd from ttkd_bsc.ct_bsc_ptm where thang_luong=18 and id=a.ptm_id)
    ;
                            

commit; 
