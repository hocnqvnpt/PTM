rename  ct_bsc_ptm_202408 to  ct_bsc_ptm_202408_lan1;

create table ttkd_bsc.ct_bsc_ptm_202409 as
	select * from ttkd_bsc.ct_bsc_ptm where thang_ptm >= 202310
	;
alter table ttkd_bsc.ct_bsc_ptm_202409 read only;
update ttkd_bsc.ct_bsc_ptm set thang_luong = thang_ptm
    where thang_luong<>thang_ptm and thang_ptm < 202410
    ;
    

-- ptm co dinh + gtgt trong thang N: 
    
insert into ttkd_bsc.ct_bsc_ptm 
                    (thang_luong, thang_ptm,ma_gd,ma_gd_gt,ma_kh,ma_tb,dich_vu, tenkieu_ld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
                    tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, datcoc_csd,ma_da,ma_duan_banhang, --chu_nhom,vnp_moi,
                    ngay_bbbg, thoihan_id, tg_thue_tu, tg_thue_den,songay_sd,
                    pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
                    hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id,pbh_db_id,trangthaitb_id, 
                    doituong_id,doituong_ct_id,doivt_id,ttvt_id, ma_tiepthi
--				,ma_tiepthi_new
				,to_tt_id,donvi_tt_id,donvi_tt,
                    nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt, ghi_chu, goi_id, tocdo_id, mucuoctb_id, tien_camket,
                    lydo_khongtinh_luong,manv_ptm,tennv_ptm
                 --   pbh_ptm_id
				,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loainv_id,ten_loainv,loai_ld,manv_hotro,tyle_hotro, tyle_am,
                    dthu_ps, dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,
                    sl_mailing, nguon, sdt_lh, email_lh, so_nha, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id,
                    nhanvien_nhan_id, chuquan_id, heso_hotro_nvptm, heso_hotro_nvhotro, nhom_tiepthi, ungdung_id)
			
        select  202409 thang_ins, 202409 thang_ptm, ma_gd, ma_gd_gt,ma_kh,ma_tb,dich_vu, ten_kieuld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
                    tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, datcoc_csd,ma_duan,ma_duan_banhang, --chu_nhom,vnp_moi,
                    ngay_bbbg, thoihan_id, tg_thue_tu, tg_thue_den, '' songay_sd,
                    pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
                    hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id, ''pbh_db_id, 
                    trangthaitb_id, doituong_id,doituong_ct_id, ''doivt_id, ''ttvt_id, ma_tiepthi
			--	,ma_tiepthi_new
				,to_tt_id,donvi_tt_id,
                    donvi_tt,nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt, trim(ghi_chu), goi_dadv_id, tocdo_id, mucuoctb_id, tien_camket,
                    lydo_khongtinh_luong,manv_ptm,tennv_ptm
                  --  , pbh_ptm_id
				,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loainv_id,ten_loainv,loai_ld,manv_hotro,tyle_hotro,tyle_am,
                    dthu_ps,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,sl_mailing, 'ptm_codinh',
                    '' sdt_lh, '' email_lh,sonha, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_id, chuquan_id, tyle_am, tyle_hotro, nhom_tiepthi, ungdung_id
        from ttkd_bct.ptm_codinh_202409 a
        where 
					 not (doituong_id = 190 and datcoc_csd is null)
		--			 doituong_id <> 190
					 and kieuld_id not in (96,13089) -- tai lap
					 and loaihd_id != 2    -- ccq
					 and dichvuvt_id <> 2 --- khong import dvu VNP tren Onebss
					 and not exists(select 1 from ttkd_bsc.ct_bsc_ptm where thang_ptm > = 202405 and hdtb_id=a.hdtb_id)
			 ;
commit
    ;
-- thue bao cdbr dung thu da chuyen sang dung that:  KIEMTRA SUBQURRY lai trong T6
	select * from ttkd_bsc.ct_bsc_ptm  where thang_luong = 4
	;
insert into ttkd_bsc.ct_bsc_ptm 
			   ( thang_luong, thang_ptm, ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
			    tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, datcoc_csd,ma_da,ma_duan_banhang, ngay_bbbg,
			    ngay_luuhs_ttkd,ngay_luuhs_ttvt,  thoihan_id, tg_thue_tu, tg_thue_den, 
			    pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
			    hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id, trangthaitb_id, doituong_id, 
			    ma_tiepthi,ma_tiepthi_new,to_tt_id,donvi_tt_id, donvi_tt,nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu,goi_id, tocdo_id, mucuoctb_id, tien_camket, lydo_khongtinh_luong,
			    manv_ptm, tennv_ptm, pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loai_ld,manv_hotro,tyle_hotro,tyle_am
--			    ngay_tt,soseri,tien_tt, ht_tra_id
			    , dthu_ps,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,sl_mailing, nop_du, nguon,
			    sdt_lh, email_lh, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_nhan_id, chuquan_id, heso_hotro_nvptm, heso_hotro_nvhotro
			    )
	
      ---thang n-3
	  select  49 thang_luong, 202409 thang_ptm, ma_gd,ma_kh,ma_tb,dich_vu,ten_kieuld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
									   tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, datcoc_csd,ma_duan,ma_duan_banhang, 
									   (select ngay_sd from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) ngay_bbbg,
									   (select ngay_bg from ttkdhcm_ktnv.v_bangiao_hoso_new where ma_gd=a.ma_gd and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id) ngay_luuhs_ttkd,
									   null ngay_luuhs_ttvt,  thoihan_id, tg_thue_tu, tg_thue_den, 
									   pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
									   hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id, 
									   (select trangthaitb_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) trangthaitb_id,
									   (select doituong_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id)  doituong_id, 
									   ma_tiepthi, null ma_tiepthi_new,to_tt_id,donvi_tt_id,
									   donvi_tt,nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu, goi_dadv_id, tocdo_id, mucuoctb_id, tien_camket, '' lydo_khongtinh_luong
									   , manv_ptm, tennv_ptm, null pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loai_ld,manv_hotro,tyle_hotro,tyle_am
									   , dthu_ps, dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,sl_mailing, 
									   (select nop_du from ttkdhcm_ktnv.v_bangiao_hoso_new where ma_gd=a.ma_gd and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id) nop_du, 
									   'ptm_codinh', '' sdt_lh, '' email_lh, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_id, chuquan_id, tyle_am heso_hotro_nvptm, tyle_hotro heso_hotro_nvhotro
					    from ttkd_bct.ptm_codinh_202406 a
					    where doituong_id=190 and chuquan_id=145
						   and not exists(select 1 from ttkd_bsc.ct_bsc_ptm where hdtb_id=a.hdtb_id)
						   and not exists(select 1 from ttkd_bsc.ct_bsc_ptm where kieuld_id=13189 and thuebao_id=a.thuebao_id)
						   and exists(select 1 from css_hcm.db_thuebao where trangthaitb_id<>7 and doituong_id<>190 and thuebao_id=a.thuebao_id) 
									  
					    union all
					    ----thang n-2
					    select  49 thang_ins, 202409 thang_ptm, ma_gd,ma_kh,ma_tb,dich_vu,ten_kieuld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
									   tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, datcoc_csd,ma_duan,ma_duan_banhang, 
									   (select ngay_sd from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) ngay_bbbg,
									   (select ngay_bg from ttkdhcm_ktnv.v_bangiao_hoso_new where ma_gd=a.ma_gd and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id) ngay_luuhs_ttkd
									   , null ngay_luuhs_ttvt
									   ,  thoihan_id, tg_thue_tu, tg_thue_den,
									   pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
									   hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id, 
									   (select trangthaitb_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) trangthaitb_id,
									   (select doituong_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id)  doituong_id, 
									   ma_tiepthi, null ma_tiepthi_new,to_tt_id,donvi_tt_id,donvi_tt,nhanviengt_id,
									   ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu,goi_dadv_id, tocdo_id, mucuoctb_id, tien_camket, '' lydo_khongtinh_luong,
									   manv_ptm, tennv_ptm, null pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loai_ld,manv_hotro,tyle_hotro,tyle_am
									   , dthu_ps,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,sl_mailing, 
									   (select nop_du from ttkdhcm_ktnv.v_bangiao_hoso_new where ma_gd=a.ma_gd and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id) nop_du, 
									   'ptm_codinh', '' sdt_lh, '' email_lh, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_id, chuquan_id, tyle_am, tyle_hotro
					    from ttkd_bct.ptm_codinh_202407 a
					    where doituong_id=190 and chuquan_id=145
						   and not exists(select 1 from ttkd_bsc.ct_bsc_ptm where hdtb_id=a.hdtb_id)
						   and not exists(select 1 from ttkd_bsc.ct_bsc_ptm where kieuld_id=13189 and thuebao_id=a.thuebao_id)
						   and exists(select 1 from css_hcm.db_thuebao where trangthaitb_id<>7 and doituong_id<>190 and thuebao_id=a.thuebao_id) 
								   
					    union 
					    ---thang n-1
					    select  49 thang_ins, 202409 thang_ptm, ma_gd,ma_kh,ma_tb,dich_vu, ten_kieuld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
								   tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, datcoc_csd,ma_duan,ma_duan_banhang, 
								   (select ngay_sd from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) ngay_bbbg,
								   (select ngay_bg from ttkdhcm_ktnv.v_bangiao_hoso_new where ma_gd=a.ma_gd and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id) ngay_luuhs_ttkd
								   , null ngay_luuhs_ttvt
								   ,  thoihan_id, tg_thue_tu, tg_thue_den,
								   pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
								   hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id, 
								   (select trangthaitb_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) trangthaitb_id,
								   (select doituong_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id)  doituong_id, 
								   ma_tiepthi, null ma_tiepthi_new,to_tt_id,donvi_tt_id,
								   donvi_tt,nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu, goi_dadv_id, tocdo_id, mucuoctb_id, tien_camket, '' lydo_khongtinh_luong,
								   manv_ptm, tennv_ptm, null pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loai_ld,manv_hotro,tyle_hotro,tyle_am
--								   , null ngay_tt, null soseri, null tien_tt, null ht_tra_id
								   , dthu_ps,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,sl_mailing, 
								   (select nop_du from ttkdhcm_ktnv.v_bangiao_hoso_new where ma_gd=a.ma_gd and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id) nop_du, 
								   'ptm_codinh', '' sdt_lh, '' email_lh, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_id, chuquan_id, tyle_am, tyle_hotro
					    from ttkd_bct.ptm_codinh_202408 a
					    where doituong_id=190 and chuquan_id=145
						   and  not exists(select 1 from ttkd_bsc.ct_bsc_ptm where hdtb_id=a.hdtb_id)
						   and not exists(select 1 from ttkd_bsc.ct_bsc_ptm where kieuld_id=13189 and thuebao_id=a.thuebao_id)
						   and exists(select 1 from css_hcm.db_thuebao where trangthaitb_id<>7 and doituong_id<>190 and thuebao_id=a.thuebao_id)  
				
	   ;
	   commit;
	   
	   ----Sau khi insert thue bao dung thu --> update tra truoc
	   MERGE INTO ttkd_bsc.ct_bsc_ptm a
	   USING (with datcoc_goc as (select b.hdtb_id, a.thuebao_id , a.thang_bd thang_bddc, a.thang_kt thang_ktdc, a.cuoc_dc datcoc_csd, a.tien_td, ctkm.huong_dc
                                                                   from css_hcm.db_datcoc a, css_hcm.hdtb_datcoc b,css_hcm.ct_khuyenmai ctkm
                                                                    where a.thuebao_dc_id=b.thuebao_dc_id and a.chitietkm_id = ctkm.chitietkm_id and ctkm.khuyenmai_id not in (1977, 2056, 2998, 2999)
                                                                                and a.cuoc_dc > 0 and a.hieuluc = 1 and a.ttdc_id = 0 and b.thang_bd >= 202405			--thang n -3
                                                                                and (a.nhom_datcoc_id not in (15,19,20,22,24) or a.nhom_datcoc_id is null)
--                                                                    group by b.hdtb_id, a.thuebao_id, a.thang_bd, a.thang_kt
                                                                            
                                                                    union all                                  
                                                                    select a.hdtb_id, a.thuebao_id, a.thang_bddc, a.thang_ktdc, a.datcoc_csd, a.tien_td, b.huong_dc
                                                                    from css_hcm.khuyenmai_dbtb a, css_hcm.ct_khuyenmai b
                                                                    where a.chitietkm_id = b.chitietkm_id and a.datcoc_csd > 0 and  a.hieuluc = 1 and a.ttdc_id = 0
                                                                                and a.khuyenmai_id not in (1977, 2056, 2998, 2999) 
                                                                                and (b.nhom_datcoc_id not in (15,19,20,22,24) or b.nhom_datcoc_id is null)                                             
--                                                                    group by a.hdtb_id, a.thuebao_id, a.thang_bddc,a.thang_ktdc     
                                                )                           
											select max(hdtb_id) hdtb_id, thuebao_id, min(thang_bddc) thang_bddc, min(thang_ktdc) thang_ktdc
																, min(huong_dc) huong_dc, sum(datcoc_csd) datcoc_csd, sum(tien_td) tien_td
                                                        from datcoc_goc 
											 where datcoc_csd > 0 and thang_bddc >= 202406 --and thuebao_id = 12211057
                                                        group by  thuebao_id
						) b
	   ON (a.thuebao_id = b.thuebao_id)
	   WHEN MATCHED THEN
		UPDATE SET a.datcoc_csd = (case when a.loaitb_id in (80,140) then b.datcoc_csd else round(b.datcoc_csd/1.1,0) end)
								, a.sothang_dc = b.huong_dc
								, a.thang_bddc = b.thang_bddc
								, a.thang_ktdc = b.thang_ktdc
								, a.thang_luong = 4 		----flag de tinh heso
		WHERE thang_luong = 49 --and ma_gd = 'HCM-LD/01663089'
		;
             commit;        

-- thay doi toc do trong thang: a Tuyen da import
				select 'ct_bsc_ptm', count(*) from ttkd_bsc.ct_bsc_ptm where thang_ptm = 202409 and nguon='thaydoitocdo'
				union all
				select 'thaydoitocdo', count(*) from ttkd_bct.thaydoitocdo_202409;
				    
				--delete from ct_bsc_ptm 
				--    where thang_ptm='202404' and nguon='thaydoitocdo_202404';
				
						
					---thang n
				insert into ttkd_bsc.ct_bsc_ptm 
								(thang_luong, thang_ptm,ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt, mst,mst_tt,
								ngay_bbbg,ngay_luuhs_ttkd, ngay_luuhs_ttvt, kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
								hdkh_id, hdtb_id, khachhang_id,thanhtoan_id,thuebao_id,trangthaitb_id,
								doituong_id,ma_tiepthi,ma_tiepthi_new, donvi_tt,manv_ptm,tennv_ptm,
								pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loainv_id,ten_loainv,loai_ld,nhom_tiepthi,
								dthu_ps_truoc, dthu_ps, dthu_goi, mien_hsgoc,ghi_chu,nguon, nhanvien_nhan_id,
								chuquan_id, ma_da, ma_duan_banhang, lydo_khongtinh_luong, tocdo_id,manv_hotro, tyle_hotro, tyle_am, heso_hotro_nvptm, heso_hotro_nvhotro)  
					   select  '202404', '202404' , ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,mst_kh, mst_tt,
								ngay_ht,ngay_luuhs_ttkd, ngay_luuhs_ttvt,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
								hdkh_id, hdtb_id, khachhang_id,thanhtoan_id,thuebao_id,trangthaitb_id, 
								doituong_id,ma_tiepthi,ma_tiepthi_new,donvi_tt, manv_ptm,tennv_ptm,
								pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,'','',loai_ld,nhom_tiepthi,
								dthu_ps_old, dthu_ps_new, dthu_duoctinh, 1,ghi_chu,'thaydoitocdo', nhanvien_id,
								chuquan_id, ma_duan, ma_duan_banhang, lydo_khongtinh_luong, tocdo_dbnew_id, manv_hotro, tyle_hotro, tyle_am, tyle_am, tyle_hotro 
					   from ttkd_bct.thaydoitocdo_202404 a
					   where not exists (select * from ttkd_bsc.ct_bsc_ptm where hdtb_id=a.hdtb_id )
								and loaitb_id!=39;
                    


-- tai lap theo vb 458/TTr-NS-DH 06/11/2020 tu ky 11/2020:
			select count(*) from ttkd_bsc.ct_bsc_ptm  
					where thang_ptm = '202409' and nguon='tailap'
					;
		---thang n
			insert into ttkd_bsc.ct_bsc_ptm 
							(thang_luong, thang_ptm,ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld
							,so_gt,mst,mst_tt,tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, ma_da,ma_duan_banhang
							,ngay_bbbg, thoihan_id, tg_thue_tu, tg_thue_den
							,pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id
							,hdkh_id, hdtb_id,khachhang_id, thanhtoan_id,thuebao_id,thuebao_cha_id
							,trangthaitb_id, doituong_id,doituong_ct_id ,ma_tiepthi,to_tt_id,donvi_tt_id
							,donvi_tt,nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu,goi_id, tien_camket
							,lydo_khongtinh_luong,manv_ptm,tennv_ptm
							,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loainv_id,ten_loainv,loai_ld, nhom_tiepthi,manv_hotro,tyle_hotro
							,dthu_ps, dthu_goi_goc,dthu_goi, tinh_id , nguon, nhanvien_nhan_id,chuquan_id, tocdo_id)
			
				   select  
							202409 thang_ins, 202409 thang_ptm, ma_gd,ma_kh,ma_tb,dich_vu,ten_kieuld tenkieu_ld,ten_tb,diachi_ld
							, so_gt,mst,mst_tt,tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, ma_duan,ma_duan_banhang
							, ngay_bbbg, thoihan_id, tg_thue_tu, tg_thue_den
							, pbh_nhan_id, pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id
							,hdkh_id, hdtb_id, khachhang_id, thanhtoan_id,thuebao_id,thuebao_cha_id 
							,trangthaitb_id, doituong_id,doituong_ct_id ,ma_tiepthi,to_tt_id,donvi_tt_id
							, donvi_tt, nhanviengt_id, ma_nguoigt, nguoi_gt, nhom_gt, ghi_chu, goi_dadv_id, tien_camket
							, lydo_khongtinh_luong
							, manv_ptm, tennv_ptm, a.ma_pb, a.ten_pb, a.ma_to, a.ten_to, a.ma_vtcv, a.loainv_id, a.ten_loainv, a.loai_ld, a.nhom_tiepthi
							,manv_hotro,tyle_hotro
							,dthu_ps,dthu_goi_goc,dthu_goi, tinh_id , 'tailap', a.nhanvien_id
							,chuquan_id, tocdo_id
				   
				   from ttkd_bct.tailap_202409 a
								--	left join ttkd_bsc.nhanvien nv on nv.ma_nv = a.manv_ptm and thang = 202405
				   where a.duoctinh_ptm = 1 and a.manv_ptm is not null
							and not exists (select 1 from ttkd_bsc.ct_bsc_ptm where thang_ptm = 202409 and loaihd_id=7 and hdtb_id=a.hdtb_id)
				;


commit;

	
-- Digishop (MyTV Mobile) ----anh Tuyen import
delete from ttkd_bsc.ct_bsc_ptm 
--	 select * from ttkd_bsc.ct_bsc_ptm
	where thang_ptm=202409 and dich_vu='MyTV MOBILE'
;      
---select * from digishop;
			insert into ttkd_bsc.ct_bsc_ptm a
							    (thang_luong, thang_ptm, dich_vu, tenkieu_ld, ma_gd, ma_tb, ten_tb, diachi_ld, sdt_lh, chuquan_id, goi_cuoc, ngay_bbbg, dthu_goi_goc, dthu_goi
							    ,khhh_khm, diaban, ma_tiepthi, ma_tiepthi_new, datcoc_csd, sothang_dc, manv_ptm, tennv_ptm, ma_to, ten_to, ma_pb, ten_pb, ma_vtcv, nhom_tiepthi
							    ,nguon, dichvuvt_id, loaitb_id, soseri, tien_tt, dthu_ps, trangthai_tt_id, heso_dichvu, trangthaitb_id, mien_hsgoc, heso_khachhang, heso_vtcv_nvptm
							    ,heso_quydinh_nvptm, heso_dichvu_dnhm, dongia, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong
							    )
					select 9 thang_luong, thang thang_ptm, danhmuc dich_vu, 'Lap dat moi MyTV OTT' tenkieu_ld, ma_don_hang ma_gd, ma_dhsx ma_tb, ten_kh ten_tb
								   ,diachi_chitiet diachi_ld, sdt_kh sdt_lh, 145 chuquan_id, tengoi goi_cuoc, ngay_kichhoat ngay_bbbg,  dthu_thucte dthu_goi_goc, dthu_thucte dthu_goi
								   , 'KHM' khhh_khm,'Khong xet trong/ngoai CT' diaban, ma_gioithieu ma_tiepthi, ma_gioithieu ma_tiepthi_new, dthu_thucte datcoc_csd, chuky/30 sothang_dc
								   ,b.ma_nv manv_ptm, b.ten_nv tennv_ptm, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb, b.ma_vtcv, b.nhomld_id nhom_tiepthi
								   ,'web Digishop' nguon, 4 dichvuvt_id, 271 loaitb_id, serial soseri, dthu_thucte tien_tt, dthu_thucte dthu_ps, 1 trangthai_tt_id
								   ,decode(chuky/30 , 0,0.2, 1,0.2, 2,0.2, 3,0.2, 4,0.2, 5,0.2, 6,0.3, 7,0.3, 8,0.3, 9,0.3, 10,0.3, 11,0.3, 12,0.4, 18,0.4, null) heso_dichvu,
								   1 trangthaitb_id, 1 mien_hsgoc,1 heso_khachhang, 1 heso_vtcv_nvptm, 1 heso_quydinh_nvptm, 0.1 heso_dichvu_dnhm
								   ,800 dongia, 202404 thang_tldg_dt, 202404 thang_tlkpi, 202404 thang_tlkpi_to, 202404 thang_tlkpi_phong        ---thang n
					from ttkd_bsc.digishop a, ttkd_bsc.nhanvien_202404 b
					where a.thang=202404
									and a.ma_gioithieu=b.ma_nv 
									and danhmuc='MyTV MOBILE' and dia_ban like 'TP H_ Ch_ Minh' and ma_gioithieu is not null 
									and trangthai_shop like 'Th_nh c_ng' and trangthai_doitac like 'Th_nh c_ng'
			   ;
			   commit;
                     
			update ttkd_bsc.ct_bsc_ptm a 
			    set (tennv_ptm, ma_to,ten_to,ma_pb,ten_pb,ma_vtcv, loai_ld, nhom_tiepthi)
					= (select b.ten_nv tennv_ptm,b.ma_to,b.ten_to,b.ma_pb,b.ten_pb,b.ma_vtcv,b.loai_ld, b.nhomld_id
						 from ttkd_bsc.nhanvien b 
						 where b.thang = a.thang_ptm and b.manv_hrm = a.ma_tiepthi) 
--		select manv_ptm,tennv_ptm, ma_to,ten_to,ma_pb,ten_pb,ma_vtcv, loainv_id, ten_loainv, loai_ld, nhom_tiepthi from ttkd_bsc.ct_bsc_ptm a 
			    where thang_ptm=202409 and loaitb_id = 271
								
			;
commit;

---DigiShop Fiber, MyTV, Mesh, VNPts

---update MANV_gioi thieu into MaNV_hotro FROM DIGISHOP 
	---dich vu VPNts theo sdt_datmua, trangthai_shop = thanh cong
				MERGE INTO ttkd_bsc.ct_bsc_ptm a
				USING (select sdt_datmua, ma_gioithieu, tennv_gioithieu from ttkd_bsc.digishop where sdt_datmua is not null and ma_gioithieu is not null and lower(trangthai_shop) like 'th_nh c_ng') b
						ON (a.ma_tb = b.sdt_datmua)
				WHEN MATCHED THEN
							UPDATE
							SET a.manv_hotro = b.ma_gioithieu
									, a.ghi_chu = ghi_chu || decode(ghi_chu, null, null, '; ') || 'donhang_digi'
									, thang_luong = 4
--									, a.nguoi_gt = b.tennv_gioithieu
--					select thang_tldg_dt, thang_luong, thang_ptm, ma_gd_gt, ma_tb,  ghi_chu, ungdung_id, nguon, manv_ptm, ma_nguoigt, nguoi_gt, manv_hotro, heso_hotro_nvhotro, luong_dongia_nvptm, luong_dongia_nvhotro from ttkd_bsc.ct_bsc_ptm a
					WHERE thang_ptm = 202409 and loaitb_id = 20 and ma_tb is not null and manv_hotro is null
									and ma_tb  in (select sdt_datmua from ttkd_bsc.digishop 
																	where sdt_datmua is not null and ma_gioithieu is not null and lower(trangthai_shop) like 'th_nh c_ng'
																				and thang >= 202408
																	)
									and nvl(thang_tldg_dt, 999999) >= 202409
--									and ma_tb = '84911180458'
					;
					rollback;
					commit;
				---update dich vu khac VPN
				---theo ma_dhsx = ma_gd_gt, trangthai_shop = thanh cong
					MERGE INTO ttkd_bsc.ct_bsc_ptm a
									USING (select ma_dhsx, ma_gioithieu, tennv_gioithieu from ttkd_bsc.digishop where lower(trangthai_shop) like 'th_nh c_ng') b
											ON (a.ma_gd_gt = b.ma_dhsx)
									WHEN MATCHED THEN
												UPDATE
												SET a.manv_hotro = b.ma_gioithieu
														, a.ghi_chu = ghi_chu || decode(ghi_chu, null, null, '; ') || 'donhang_digi'
														, thang_luong = 4
													--	, a.nguoi_gt = b.tennv_gioithieu
--										select thang_tldg_dt, thang_luong, ngay_bbbg, thang_ptm, ma_gd_gt, ma_tb,  ghi_chu, ungdung_id, nguon, manv_ptm, ma_nguoigt, nguoi_gt, manv_hotro, heso_hotro_nvhotro, luong_dongia_nvptm, luong_dongia_nvhotro from ttkd_bsc.ct_bsc_ptm a
										WHERE thang_ptm = 202409 and ma_gd_gt is not null and a.manv_hotro is null
														and ma_gd_gt  in (select ma_dhsx from ttkd_bsc.digishop 
																						where ma_gioithieu is not null and ma_dhsx is not null and lower(trangthai_shop) like 'th_nh c_ng'
																										and thang >= 202408
																					)
														and nvl(thang_tldg_dt, 999999) >= 202409
--														and ma_gd_gt in ('HCM-GT/00157123',
--'HCM-GT/00158527',
--'HCM-GT/00158908',
--'HCM-GT/00159113')
										;
---SHOP_CTV Fiber, MyTV, Mesh
				---theo ma_dhsx = ma_gd_gt, trangthai_shop = thanh cong
				Select * From khanhtdt_ttkd.IMP_SHOPCTV_DH_2024 where thang = 202409 and VAITRO_CTV = 'CTV liên kết' and MA_DHSXKD is not null
				;
					MERGE INTO ttkd_bsc.ct_bsc_ptm a
									USING (select ma_dhsxkd, ma_nvkd, ten_nvkd, sodt_nvkd 
																from khanhtdt_ttkd.IMP_SHOPCTV_DH_2024 
																where thang = 202409 and VAITRO_CTV = 'CTV liên kết' and MA_DHSXKD is not null) b
											ON (a.ma_gd_gt = b.ma_dhsxkd)
									WHEN MATCHED THEN
												UPDATE
												SET a.manv_hotro = b.ma_nvkd
														, a.ghi_chu = ghi_chu || decode(ghi_chu, null, null, '; ') || 'donhang_shopctv_CTV_LK'
														, thang_luong = 4
--														, thang_tldg_dt_nvhotro = 202410, thang_tlkpi_hotro = 202410
													--	, a.nguoi_gt = b.tennv_gioithieu
--										select thang_tldg_dt, thang_luong, ngay_bbbg, thang_ptm, ma_gd_gt, ma_tb,  ghi_chu, ungdung_id, nguon, manv_ptm, ma_nguoigt, nguoi_gt, manv_hotro, heso_hotro_nvhotro, luong_dongia_nvptm, luong_dongia_nvhotro from ttkd_bsc.ct_bsc_ptm a
										WHERE thang_ptm = 202409 and ma_gd_gt is not null and a.manv_hotro is null
														and ma_gd_gt  in (select ma_dhsxkd from khanhtdt_ttkd.IMP_SHOPCTV_DH_2024
																						where VAITRO_CTV = 'CTV liên kết' and MA_DHSXKD is not null
																										and thang >= 202408
																					)
														and nvl(thang_tldg_dt, 999999) >= 202409
;
commit;
rollback;
				select thang_ptm, ma_gd_gt, ma_tb, dich_vu, ghi_chu, ungdung_id, nguon, manv_ptm, manv_hotro, ma_gioithieu
							, heso_hotro_nvptm, heso_hotro_nvhotro, dthu_goi, luong_dongia_nvptm, luong_dongia_nvhotro, lydo_khongtinh_dongia, thang_tldg_dt, thang_tlkpi
				from ttkd_bsc.ct_bsc_ptm a
								left join (select sdt_datmua, ma_gioithieu from ttkd_bsc.digishop where sdt_datmua is not null and lower(trangthai_shop) like 'th_nh c_ng') b
											on a.ma_tb = b.sdt_datmua
				WHERE a.thang_ptm = 202407 and thang_luong = 4 and a.loaitb_id = 20 and a.ma_tb is not null
			
				union all
				
				select thang_ptm, ma_gd_gt, ma_tb,  dich_vu, ghi_chu, ungdung_id, nguon, manv_ptm, manv_hotro, ma_gioithieu
								, heso_hotro_nvptm, heso_hotro_nvhotro, dthu_goi, luong_dongia_nvptm, luong_dongia_nvhotro
								, lydo_khongtinh_dongia, thang_tldg_dt, thang_tlkpi
				from ttkd_bsc.ct_bsc_ptm a
									 left join (select ma_dhsx, ma_gioithieu from ttkd_bsc.digishop where ma_dhsx is not null and lower(trangthai_shop) like 'th_nh c_ng') b
											on a.ma_gd_gt = b.ma_dhsx
				WHERE thang_ptm = 202407 and thang_luong = 4 and dichvuvt_id not in (7,8,9, 14,15,16, 2)
							
			;				
		----
		select * 
		from ttkd_bsc.ct_bsc_ptm a
				WHERE a.thang_ptm = 202406 and a.GHI_CHU = 'donghang_digi'
