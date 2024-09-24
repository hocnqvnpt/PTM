Theo VB 977/VNPT.TPHCM-PTTT ngay 29/3/2022, quy dinh co che kinh te san pham dich vu cong nghe thong tin (so doanh nghiep) giua VNPT TP Ho Chi Minh 
va Trung tam Kinh doanh TP Ho Chi Minh

 select * from ct_ptm_phanchia_dthu;          

update ct_ptm_phanchia_dthu a set (ten_tb, diachi_ld, ngay_bbbg, dichvuvt_id, loaitb_id)=
            (select bb.ten_tb, bb.diachi_ld, bb.ngay_ht, bb.dichvuvt_id, bb.loaitb_id from css_hcm.hd_khachhang aa, css_hcm.hd_thuebao bb
                where aa.hdkh_id=bb.hdkh_id and aa.loaihd_id=1
                            and aa.ma_gd=a.ma_gd and bb.ma_tb=a.ma_tb)
     where ngay_bbbg is null;

update ct_bsc_ptm a set (thang_luong, tyle_huongdt)=
            (select 11, tyle_ttkd from ct_ptm_phanchia_dthu where ma_gd=a.ma_gd and ma_tb=a.ma_tb)
        where thang_ptm='202205' and exists  (select tyle_ttkd from ct_ptm_phanchia_dthu where ma_gd=a.ma_gd and ma_tb=a.ma_tb);

       
       
                 -- Doanh thu don gia cac dv ngoai tru vnptt, SMS Brandname:
            update ct_bsc_ptm a set 
                        doanhthu_dongia_nvptm = round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                            *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                            *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                            *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
                        ,doanhthu_dongia_nvhotro =null --dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvhotro*heso_vtcv_nvhotro*heso_hotro_nvhotro*heso_khachhang
                        ,doanhthu_dongia_dai = round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_dai*heso_vtcv_dai*heso_hotro_dai*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ,0)
            -- select thang_ptm, ma_tb, ma_vtcv, dthu_goi, tyle_hotro, heso_dichvu, heso_hotro_nvhotro, heso_quydinh_nvptm,heso_vtcv_nvptm, heso_khuyenkhich, heso_tratruoc, heso_kvdacthu, doanhthu_dongia_nvptm, round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)*nvl(tyle_huongdt,1) ,0) dthu_dongia_new, thang_tldg_dt from ct_bsc_ptm
            where thang_luong='11';
                       
            commit;    
            
                -- Doanh thu goi tich hop: ap dung khong phan biet tap quan ly (theo VB 275/TTr-NS-DH 22/06/2020)
                            -- tham khao bang huong dan cua anh Nghia tinh he so cac goi tich hop cho SMEs: VB 275/TTKD HCM-DH 22/06/2020:
            update ct_bsc_ptm a set doanhthu_dongia_nvptm =
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
            -- select ma_tb, dthu_goi, doanhthu_dongia_nvptm, dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1) dthu_dongia_new from ct_bsc_ptm
            where thang_luong='1' and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605);
                        
                        
            commit;                            
                
            
                 -- Luong don gia cac dv ngoai tru vnptt, SMS Brandname:
            update ct_bsc_ptm a set luong_dongia_nvptm=nvl(doanhthu_dongia_nvptm,0)*dongia/1000,
                                                                            luong_dongia_nvhotro = null,  
                                                                            luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
                 -- select thang_ptm, ma_tb, heso_dichvu, doanhthu_dongia_nvptm, luong_dongia_nvptm from ct_bsc_ptm a
                 where thang_luong='11';
                            
                             
                commit;            
            
            -- SMS Brandname: Tinh lai dthu don gia dv sms brandname cua thang n-1: 
            --ma_tb<>'hcm_brn_00009121';  --  VB 1254 TTr-?H-NS
            update ct_bsc_ptm a set 
                        doanhthu_dongia_nvptm =( (nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                                *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm
                                                                                *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)
                                                                                *heso_khachhang*nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)
                        ,doanhthu_dongia_nvhotro =null  -- dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvhotro*heso_vtcv_nvhotro*heso_hotro_nvhotro*heso_khachhang
                        ,doanhthu_dongia_dai =null
            -- select ma_tb, dthu_goi, dthu_goi_ngoaimang, heso_dichvu, heso_dichvu_1, thang_tldg_dt, thang_tlkpi_phong,lydo_khongtinh_luong from ct_bsc_ptm 
            where thang_luong='5' and loaitb_id=131;
                       
                         
            commit;
            
            update ct_bsc_ptm set luong_dongia_nvptm=nvl(doanhthu_dongia_nvptm,0)*dongia/1000,
                                                                             luong_dongia_nvhotro = null,  -- nvl(doanhthu_dongia_nvhotro,0)*dongia/1000,    
                                                                             luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
                 where thang_luong='5' and loaitb_id=131;
                           
            commit;
            
  
--====== DOANH THU KPI  CA NHAN =======:

            update ct_bsc_ptm a
                        set doanhthu_kpi_nvptm= round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                            *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                            *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                            *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) *nvl(tyle_huongdt,1),0)
                -- select dich_vu, ma_gd, ma_tb, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, heso_khachhang, heso_vtcv_nvptm,heso_hotro_nvptm, *nvl(tyle_huongdt,1),doanhthu_kpi_nvptm,lydo_khongtinh_dongia, thang_tldg_dt from ct_bsc_ptm a 
                where thang_luong='11';
                            
            commit;


           -- dthu kpi cua dich vu giai phap thiet bi, giai phap CNTT: 
                -- dthu tinh bsc la dthu hop dong x cac he so tinh bsc (ko tinh tren chenh lech thu chi).
            update ct_bsc_ptm a
                        set doanhthu_kpi_nvptm= round(dthu_goi_goc*nvl(heso_dichvu,1)*nvl(heso_dichvu_1,1)*nvl(heso_quydinh_nvptm,1)
                                                      *decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) -- *decode(tyle_hotro,null,1,1-tyle_hotro)
                                                      --*decode(manv_tt_dai,null,1,0.5) 
                                                      *nvl(heso_tbnganhan,1)
                                                      *nvl(heso_diaban_tinhkhac,1)  ,0)
                -- select dich_vu, ma_gd, ma_tb, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, heso_khachhang, heso_vtcv_nvptm,heso_hotro_nvptm, doanhthu_kpi_nvptm,lydo_khongtinh_dongia, thang_tldg_dt from ct_bsc_ptm a 
                where thang_luong='5' and bo_dau(dich_vu) like '%giai phap%';
                            
                            
            commit;
           
           -- kpi goi SME: ko xet heso_quydinh_nvptm vi ko phan biet tap KH
           update ct_bsc_ptm a
                        set doanhthu_kpi_nvptm=round(dthu_goi*nvl(heso_dichvu,1)*nvl(heso_quydinh_nvptm,1)
                                                      *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                      *nvl(heso_kvdacthu,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                      *decode(tyle_hotro,null,1,1-tyle_hotro)*decode(manv_tt_dai,null,1,0.5)  ,0)
                                                      *nvl(heso_diaban_tinhkhac,1)
                where thang_luong='5' and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605);
           commit;
           

               -- kpi nv ho tro:
            update ct_bsc_ptm a
                       set doanhthu_kpi_nvhotro=round(dthu_goi*heso_dichvu*nvl(heso_quydinh_nvhotro,1)
                                                                        *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                        *nvl(heso_kvdacthu,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                        *nvl(heso_diaban_tinhkhac,1)*tyle_hotro   ,0)
                -- select ma_tb, dthu_goi, manv_hotro, tyle_hotro,  doanhthu_kpi_nvhotro, thang_tlkpi_hotro from ct_bsc_ptm a
                where thang_luong='6' and manv_hotro is not null;
                          
            commit;
            

           -- dthu kpi nv ho tro cua dich vu giai phap thiet bi, giai phap CNTT: 
                -- dthu tinh bsc la dthu hop dong x cac he so tinh bsc (ko tinh tren chenh lech thu chi).
            update ct_bsc_ptm a
                        set doanhthu_kpi_nvhotro= round(dthu_goi_goc*nvl(heso_dichvu,1)*nvl(heso_quydinh_nvhotro,1)*tyle_hotro ,0)
                                                                                *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)
                -- select dich_vu, ma_gd, ma_tb, manv_hotro, tyle_hotro, dthu_goi_goc, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, heso_hotro_nvhotro, doanhthu_kpi_nvptm,doanhthu_kpi_nvhotro, lydo_khongtinh_dongia from ct_bsc_ptm a 
                where thang_luong='5' and bo_dau(dich_vu) like '%giai phap%';
                            
                            
            commit;
            
   
              -- SMS Brandname thang n-1:
            update ct_bsc_ptm
                        set doanhthu_kpi_nvptm= round(  ( (nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
                                                      *nvl(heso_quydinh_nvptm,1)
                                                      *decode(tyle_hotro,null,1,1-tyle_hotro)*decode(manv_tt_dai,null,1,0.5)  ,0)
                                                      *nvl(heso_diaban_tinhkhac,1)
                -- select dthu_goi, dthu_goi_ngoaimang, doanhthu_kpi_nvptm from ct_bsc_ptm
                where thang_luong='5' and loaitb_id=131;
                            
            commit;
            
              -- SMS Brandname thang n-1 cua nv ho tro:
            update ct_bsc_ptm
               set doanhthu_kpi_nvhotro=round(   ( (nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
                                                                        *nvl(heso_quydinh_nvhotro,1)*tyle_hotro ,0)*nvl(heso_diaban_tinhkhac,1)
             
             where thang_luong='5' and loaitb_id=131 and manv_hotro is not null ;
            commit;
            
                -- Kpi nv dai:
            update ct_bsc_ptm
                                set doanhthu_kpi_nvdai=doanhthu_dongia_dai
                     -- select * from ct_bsc_ptm
                     where thang_luong='5' and manv_tt_dai is not null; 
                     
            commit;



-- ========= DOANH THU KPI TO, PHONG =========:

            update ct_bsc_ptm a
                        set doanhthu_kpi_to= round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                            *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                            *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                            *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
                -- select dich_vu, ma_gd, ma_tb, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, doanhthu_kpi_nvptm,doanhthu_kpi_phong, lydo_khongtinh_dongia, thang_tldg_dt from ct_bsc_ptm a 
                where thang_luong='11';
                
            update ct_bsc_ptm a
                        set doanhthu_kpi_phong= round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                            *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                            *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                            *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
                -- select dich_vu, ma_gd, ma_tb, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, doanhthu_kpi_nvptm,doanhthu_kpi_phong, lydo_khongtinh_dongia, thang_tldg_dt from ct_bsc_ptm a 
                where thang_luong='11';
                            
            commit;
            

           -- dthu kpi cua dich vu giai phap thiet bi, giai phap CNTT: 
                -- dthu tinh bsc la dthu hop dong x cac he so tinh bsc (ko tinh tren chenh lech thu chi). => dthu_goi_goc
            update ct_bsc_ptm a
                        set doanhthu_kpi_to= round(dthu_goi_goc*nvl(heso_dichvu,1)*nvl(heso_dichvu_1,1)  
                                                      *decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) *nvl(heso_tbnganhan,1) *nvl(heso_diaban_tinhkhac,1)  ,0)
                                                      
                              doanhthu_kpi_phong= round(dthu_goi_goc*nvl(heso_dichvu,1)*nvl(heso_dichvu_1,1)  
                                                      *decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) *nvl(heso_tbnganhan,1) *nvl(heso_diaban_tinhkhac,1)  ,0)                      
                -- select dich_vu, ma_gd, ma_tb, dthu_goi_goc, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, doanhthu_kpi_nvptm,doanhthu_kpi_phong, lydo_khongtinh_dongia from ct_bsc_ptm a 
                where thang_luong='5' and bo_dau(dich_vu) like '%giai phap%';
                            
                            
            commit;
            
            
           -- kpi goi SME: ko xet heso_quydinh_nvptm vi ko phan biet tap KH
           update ct_bsc_ptm a
                        set doanhthu_kpi_phong=round(dthu_goi*nvl(heso_dichvu,1)
                                                      *decode(tyle_hotro,null,1,1-tyle_hotro)*decode(manv_tt_dai,null,1,0.5)  ,0)*nvl(heso_tbnganhan,1)
                                                      *nvl(heso_diaban_tinhkhac,1)
                where thang_luong='1' and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605);
           commit;
           
   
              -- SMS Brandname thang n-1:
            update ct_bsc_ptm
                        set doanhthu_kpi_phong= round(  ( (nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
                                                      *decode(tyle_hotro,null,1,1-tyle_hotro)*decode(manv_tt_dai,null,1,0.5)  ,0)
                                                      *nvl(heso_diaban_tinhkhac,1)
                -- select dthu_goi, dthu_goi_ngoaimang, doanhthu_kpi_nvptm from ct_bsc_ptm
                where thang_luong='1' and loaitb_id=131;
                            
            commit;
            
          
            
    select thang_ptm, ma_tb, dthu_goi, doanhthu_dongia_nvptm, doanhthu_kpi_nvptm, doanhthu_kpi_to, doanhthu_kpi_phong,
                thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong
            from ct_bsc_ptm
            where thang_luong='11';