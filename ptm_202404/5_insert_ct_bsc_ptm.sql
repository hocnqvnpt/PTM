update ttkd_bsc.ct_bsc_ptm set thang_luong=thang_ptm
    where thang_luong<>thang_ptm 
    ;
    commit;


-- ptm co dinh + gtgt trong thang: 
delete from ct_bsc_ptm
    where thang_ptm=202404 and nguon='ptm_codinh_202404';
    
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
        select  '202404' thang_ins, '202404' thang_ptm, ma_gd, ma_gd_gt,ma_kh,ma_tb,dich_vu, ten_kieuld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
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
                    dthu_ps,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,sl_mailing, 'ptm_codinh_202404',
                    '' sdt_lh, '' email_lh,sonha, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_id, chuquan_id, tyle_am, tyle_hotro, nhom_tiepthi, ungdung_id
        from ttkd_bct.ptm_codinh_202404 a
        where 
                doituong_id<>190 
                and kieuld_id not in (96,13089) -- tai lap
                and loaihd_id!=2    -- ccq
                and not exists(select 1 from ttkd_bsc.ct_bsc_ptm where hdtb_id=a.hdtb_id)
			 ;
--			 update  ttkd_bsc.ct_bsc_ptm a  set a.manv_hotro = (select manv_hotro from ttkd_bct.ptm_codinh_202404 where hdtb_id = a.hdtb_id)
--																		, a.tyle_hotro = (select tyle_hotro from ttkd_bct.ptm_codinh_202404 where hdtb_id = a.hdtb_id)
--																		, a.tyle_am = (select tyle_hotro from ttkd_bct.ptm_codinh_202404 where hdtb_id = a.hdtb_id)
--	--		 select manv_hotro, tyle_hotro, tyle_am from ttkd_bsc.ct_bsc_ptm a
--			 where a.ma_gd = '00798153'
                ;

-- thue bao cdbr dung thu da chuyen sang dung that:
insert into ttkd_bsc.ct_bsc_ptm 
			   ( thang_luong, thang_ptm, ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
			    tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, datcoc_csd,ma_da,ma_duan_banhang, ngay_bbbg,
			    ngay_luuhs_ttkd,ngay_luuhs_ttvt,  thoihan_id, tg_thue_tu, tg_thue_den, 
			    pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
			    hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id, trangthaitb_id, doituong_id, 
			    ma_tiepthi,ma_tiepthi_new,to_tt_id,donvi_tt_id, donvi_tt,nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu,goi_id, tocdo_id, mucuoctb_id, tien_camket, lydo_khongtinh_luong,
			    manv_ptm, tennv_ptm, pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loai_ld,manv_hotro,tyle_hotro,tyle_am,
			    ngay_tt,soseri,tien_tt, ht_tra_id, dthu_ps,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,sl_mailing, nop_du, nguon,
			    sdt_lh, email_lh, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_nhan_id, chuquan_id, heso_hotro_nvptm, heso_hotro_nvhotro
			    )
      
    select  4 thang_luong, 202401 thang_ptm, ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
				   tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, datcoc_csd,ma_duan,ma_duan_banhang, 
				   (select ngay_sd from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) ngay_bbbg,
				   (select ngay_bg from ttkdhcm_ktnv.v_bangiao_hoso_new where ma_gd=a.ma_gd and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id) ngay_luuhs_ttkd,
				   ngay_luuhs_ttvt,  thoihan_id, tg_thue_tu, tg_thue_den, 
				   pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
				   hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id, 
				   (select trangthaitb_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) trangthaitb_id,
				   (select doituong_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id)  doituong_id, 
				   ma_tiepthi,ma_tiepthi_new,to_tt_id,donvi_tt_id,
				   donvi_tt,nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu,goi_id, tocdo_id, mucuoctb_id, tien_camket, '' lydo_khongtinh_luong,
				   manv_ptm, tennv_ptm, pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loai_ld,manv_hotro,tyle_hotro,tyle_am,
				   ngay_tt,soseri,tien_tt, ht_tra_id, dthu_ps,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,sl_mailing, 
				   (select nop_du from ttkdhcm_ktnv.v_bangiao_hoso_new where ma_gd=a.ma_gd and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id) nop_du, 
				   'ptm_codinh_202401', '' sdt_lh, '' email_lh, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_id, chuquan_id, tyle_am heso_hotro_nvptm, tyle_hotro heso_hotro_nvhotro
    from ttkd_bct.ptm_codinh_202401 a
    where doituong_id=190 and chuquan_id=145
        and not exists(select 1 from ct_bsc_ptm where hdtb_id=a.hdtb_id)
        and not exists(select 1 from ct_bsc_ptm where kieuld_id=13189 and thuebao_id=a.thuebao_id)
        and exists(select 1 from css_hcm.db_thuebao where trangthaitb_id<>7 and doituong_id<>190 and thuebao_id=a.thuebao_id) 
                      
    union all
    select  4 thang_ins, 202402 thang_ptm, ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
				   tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, datcoc_csd,ma_duan,ma_duan_banhang, 
				   (select ngay_sd from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) ngay_bbbg,
				   (select ngay_bg from ttkdhcm_ktnv.v_bangiao_hoso_new where ma_gd=a.ma_gd and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id) ngay_luuhs_ttkd
				   , null ngay_luuhs_ttvt
				   ,  thoihan_id, tg_thue_tu, tg_thue_den,
				   pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
				   hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id, 
				   (select trangthaitb_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) trangthaitb_id,
				   (select doituong_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id)  doituong_id, 
				   ma_tiepthi,ma_tiepthi_new,to_tt_id,donvi_tt_id,donvi_tt,nhanviengt_id,
				   ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu,goi_id, tocdo_id, mucuoctb_id, tien_camket, '' lydo_khongtinh_luong,
				   manv_ptm, tennv_ptm, pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loai_ld,manv_hotro,tyle_hotro,tyle_am,
				   ngay_tt,soseri,tien_tt, ht_tra_id, dthu_ps,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,sl_mailing, 
				   (select nop_du from ttkdhcm_ktnv.v_bangiao_hoso_new where ma_gd=a.ma_gd and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id) nop_du, 
				   'ptm_codinh_202402', '' sdt_lh, '' email_lh, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_id, chuquan_id, tyle_am, tyle_hotro
    from ttkd_bct.ptm_codinh_202402 a
    where doituong_id=190 and chuquan_id=145
        and not exists(select 1 from ct_bsc_ptm where hdtb_id=a.hdtb_id)
        and not exists(select 1 from ct_bsc_ptm where kieuld_id=13189 and thuebao_id=a.thuebao_id)
        and exists(select 1 from css_hcm.db_thuebao where trangthaitb_id<>7 and doituong_id<>190 and thuebao_id=a.thuebao_id) 
                  
    union 
    select  4 thang_ins, 202403 thang_ptm, ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
			   tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, datcoc_csd,ma_duan,ma_duan_banhang, 
			   (select ngay_sd from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) ngay_bbbg,
			   (select ngay_bg from ttkdhcm_ktnv.v_bangiao_hoso_new where ma_gd=a.ma_gd and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id) ngay_luuhs_ttkd
			   , null ngay_luuhs_ttvt
			   ,  thoihan_id, tg_thue_tu, tg_thue_den,
			   pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
			   hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id, 
			   (select trangthaitb_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) trangthaitb_id,
			   (select doituong_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id)  doituong_id, 
			   ma_tiepthi,ma_tiepthi_new,to_tt_id,donvi_tt_id,
			   donvi_tt,nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu,goi_id, tocdo_id, mucuoctb_id, tien_camket, '' lydo_khongtinh_luong,
			   manv_ptm, tennv_ptm, pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loai_ld,manv_hotro,tyle_hotro,tyle_am
			   , null ngay_tt, null soseri, null tien_tt, null ht_tra_id
			   , dthu_ps,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,sl_mailing, 
			   (select nop_du from ttkdhcm_ktnv.v_bangiao_hoso_new where ma_gd=a.ma_gd and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id) nop_du, 
			   'ptm_codinh_202403', '' sdt_lh, '' email_lh, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_id, chuquan_id, tyle_am, tyle_hotro
    from ttkd_bct.ptm_codinh_202403 a
    where doituong_id=190 and chuquan_id=145
        and not exists(select 1 from ct_bsc_ptm where hdtb_id=a.hdtb_id)
        and not exists(select 1 from ct_bsc_ptm where kieuld_id=13189 and thuebao_id=a.thuebao_id)
        and exists(select 1 from css_hcm.db_thuebao where trangthaitb_id<>7 and doituong_id<>190 and thuebao_id=a.thuebao_id)  
	   
	   ;
                     

-- thay doi toc do trong thang:
select 'ct_bsc_ptm', count(*) from ttkd_bsc.ct_bsc_ptm where nguon='thaydoitocdo_202404'
union all
select 'thaydoitocdo_202404', count(*) from ttkd_bct.thaydoitocdo_202404;
    
delete from ct_bsc_ptm 
    where thang_ptm='202404' and nguon='thaydoitocdo_202404';
commit;


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
                    dthu_ps_old, dthu_ps_new, dthu_duoctinh, 1,ghi_chu,'thaydoitocdo_202404', nhanvien_id,
                    chuquan_id, ma_duan, ma_duan_banhang, lydo_khongtinh_luong, tocdo_dbnew_id, manv_hotro, tyle_hotro, tyle_am, tyle_am, tyle_hotro 
        from ttkd_bct.thaydoitocdo_202404 a
        where not exists (select * from ttkd_bsc.ct_bsc_ptm where hdtb_id=a.hdtb_id )
                    and loaitb_id!=39;
                
               
            
commit; 



--insert into ttkd_bsc.ct_bsc_ptm 
--                    (thang_luong, thang_ptm,ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt, mst,mst_tt,
--                    ngay_bbbg,ngay_luuhs_ttkd, ngay_luuhs_ttvt, kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
--                    hdkh_id, hdtb_id, khachhang_id,thanhtoan_id,thuebao_id,pbh_db_id,trangthaitb_id,
--                    doituong_id,ma_tiepthi,ma_tiepthi_new,
--                    donvi_tt,manv_ptm,tennv_ptm,
--                    pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loainv_id,ten_loainv,loai_ld,
--                    dthu_ps_truoc, dthu_ps, dthu_goi, mien_hsgoc,ghi_chu,nguon, nhanvien_nhan_id,
--                    chuquan_id, ma_da, ma_duan_banhang, lydo_khongtinh_luong, tocdo_id,manv_hotro, tyle_hotro, tyle_am, heso_hotro_nvptm, heso_hotro_nvhotro)  
--        select  '7', '202309' , ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,mst_kh, mst_tt,
--                    ngay_ht,ngay_luuhs_ttkd, ngay_luuhs_ttvt,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
--                    hdkh_id, hdtb_id, khachhang_id,thanhtoan_id,thuebao_id,pbh_db_id,trangthaitb_id, 
--                    doituong_id,ma_tiepthi,ma_tiepthi_new,donvi_tt,
--                    ,manv_ptm,tennv_ptm,
--                    pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loainv_id,ten_loainv,loai_ld,
--                    dthu_ps_old, dthu_ps_new, dthu_duoctinh, 1,ghi_chu,'thaydoitocdo_202309', nhanvien_id,
--                    chuquan_id, ma_duan, ma_duan_banhang, lydo_khongtinh_luong, TOCDO_DBNEW_ID, manv_hotro, tyle_hotro, tyle_am, tyle_am, tyle_hotro
--            -- select *  
--        from ttkd_bct.thaydoitocdo_202309 a
--        where ma_gd='HCM-TD/00610086';
               
commit;               

-- ID447			------chay dot 2 vi doi anh Nghia duyet , chay file 3_447 moi chay.
delete from ttkd_bsc.ct_bsc_ptm 
		-- select * from ttkd_bsc.ct_bsc_ptm 
        where thang_ptm='202404' and ma_kh='GTGT rieng';
               
insert into ttkd_bsc.ct_bsc_ptm 
					   (thang_luong, thang_ptm, id_447, ma_gd,ma_kh,ma_tb,sohopdong,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,mst,
					   sothang_dc,ngay_bbbg,ngay_luuhs_ttkd,dichvuvt_id,loaitb_id,trangthaitb_id,doituong_id,                
					   ma_tiepthi,ma_tiepthi_new,donvi_tt,ghi_chu,manv_hotro, tyle_hotro,ma_dt_kh,manv_ptm,tennv_ptm,
					   pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loai_ld,
					   ngay_tt,soseri,tien_tt,dthu_ps, dthu_goi_goc,dthu_goi,heso_dichvu,ma_duan_banhang,nguon, chuquan_id
					   ,heso_hotro_nvptm, heso_hotro_nvhotro, xacnhan_khkt, thang_xacnhan_khkt)
    select 202404 thang_ins, 202404 thang_ptm, id, case when ma_gd is not null then ma_gd else sohopdong end ma_gd, 'GTGT rieng', ma_tb
				   ,sohopdong, dich_vu, 'Dat moi hop dong GTGT/CNTT', ten_kh, diachi_kh,so_gt,mst,sothang, ngay_nghiemthu, ngay_nghiemthu ngay_luuhs_ttkd, dichvuvt_id,loaitb_id
				   ,1,21 doituong_id, manv_ptm, manv_ptm, ten_pb, ghi_chu
				   ,(select manv_hotro from ttkd_bct.ptm_gtgt where id=a.id and rownum=1) manv_hotro
				   ,(select tyle_hotro from ttkd_bct.ptm_gtgt where id=a.id and rownum=1) tyle_hotro 
				   ,'dn' ma_dt_kh,manv_ptm, tennv_ptm, pbh_ptm_id, ma_pb, ten_pb, ma_to, ten_to, ma_vtcv, loai_ld
				   ,ngay_tt, sohoadon, tien_tt, dthu_ps, dthu_theohopdong, dthu_goi, heso_dichvu
				   ,ma_duan_banhang, nguon, chuquan_id, tyle_am, tyle_hotro, tien_tt_khkt, to_char(ngaycapnhat_khkt, 'yyyymm')
    from ttkd_bct.ptm_gtgt a
    where thang=202404 and not exists(select 1 from ttkd_bsc.ct_bsc_ptm where id_447=a.id) 
    ;

commit;


-- tai lap theo vb 458/TTr-NS-DH 06/11/2020 tu ky 11/2020:
			select count(*) from ttkd_bsc.ct_bsc_ptm  
					where thang_ptm='202404' and nguon='tailap_202404'
					;
    
insert into ttkd_bsc.ct_bsc_ptm 
                    (thang_luong, thang_ptm,ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld
                    ,so_gt,mst,mst_tt,tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, ma_da,ma_duan_banhang
                    ,ngay_bbbg, thoihan_id, tg_thue_tu, tg_thue_den
                    ,pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id
                    ,hdkh_id, hdtb_id,khachhang_id, thanhtoan_id,thuebao_id,thuebao_cha_id
                    ,trangthaitb_id, doituong_id,doituong_ct_id ,ma_tiepthi,to_tt_id,donvi_tt_id
                    ,donvi_tt,nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu,goi_id, tien_camket
                    ,lydo_khongtinh_luong,manv_ptm,tennv_ptm
                    ,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loainv_id,ten_loainv,loai_ld,manv_hotro,tyle_hotro
                    ,dthu_ps, dthu_goi_goc,dthu_goi, tinh_id , nguon, nhanvien_nhan_id,chuquan_id, tocdo_id)

        select  '202404' thang_ins, '202404' thang_ptm, ma_gd,ma_kh,ma_tb,dich_vu,ten_kieuld tenkieu_ld,ten_tb,diachi_ld
                    , so_gt,mst,mst_tt,tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, ma_duan,ma_duan_banhang
                    , ngay_bbbg, thoihan_id, tg_thue_tu, tg_thue_den
                    , pbh_nhan_id, pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id
                    ,hdkh_id, hdtb_id, khachhang_id, thanhtoan_id,thuebao_id,thuebao_cha_id 
                    ,trangthaitb_id, doituong_id,doituong_ct_id ,ma_tiepthi,to_tt_id,donvi_tt_id
                    , donvi_tt, nhanviengt_id, ma_nguoigt, nguoi_gt, nhom_gt, ghi_chu, goi_dadv_id, tien_camket
                    , lydo_khongtinh_luong, manv_ptm
                    , nv.ten_nv
                    , a.ma_pb, a.ten_pb, a.ma_to, a.ten_to, a.ma_vtcv, a.loainv_id, a.ten_loainv, a.loai_ld,manv_hotro,tyle_hotro
                    ,dthu_ps,dthu_goi_goc,dthu_goi, tinh_id , 'tailap_202404', a.nhanvien_id
                    ,chuquan_id, tocdo_id
        
        from ttkd_bct.tailap_202404 a
						left join ttkd_bsc.nhanvien_202404 nv on nv.ma_nv = a.manv_ptm
        where a.duoctinh_ptm = 1 and a.manv_ptm is not null
                    and not exists (select 1 from ttkd_bsc.ct_bsc_ptm where thang_ptm='202404' and loaihd_id=7 and hdtb_id=a.hdtb_id)
	;


commit;


---- ccq: vb 380, bay gio chuyen mo hinh CSKH thi 381 con hieu luc khong
			--select 'ct_bsc_ptm', count(*) from ttkd_bsc.ct_bsc_ptm where nguon='ccq_202404'
			--union all
			--select 'ccq_202404', count(*) from ttkd_bct.ccq_202404 where ketqua like '%Duoc tinh ptm%';
			--
			--    
			--insert into ttkd_bsc.ct_bsc_ptm 
			--                    (thang_luong, thang_ptm,ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,
			--                    tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, datcoc_csd,ma_da,ma_duan_banhang,
			--                    ngay_bbbg,ngay_luuhs_ttkd,ngay_luuhs_ttvt, 
			--                    pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
			--                    hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id,
			--                    trangthaitb_id, doituong_id,doituong_ct_id, ma_tiepthi,ma_tiepthi_new,to_tt_id,donvi_tt_id,donvi_tt,
			--                    nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu,goi_id, tien_camket  ,
			--                    lydo_khongtinh_luong,manv_ptm,tennv_ptm,
			--                    pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loainv_id,ten_loainv,loai_ld,manv_hotro,tyle_hotro,
			--                    ngay_tt,soseri,tien_tt, ht_tra_id, dthu_ps, dthu_goi_goc,dthu_goi,nop_du,nguon, 
			--                    so_nha, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_nhan_id, chuquan_id, tocdo_id)
			--            
			--        select  '202404' thang_ins, '202404' thang_ptm, 
			--                    ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,--mst, mst_tt,
			--                    tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc, datcoc_csd,ma_duan,ma_duan_banhang,
			--                    ngay_bbbg,ngay_luuhs_ttkd,ngay_luuhs_ttvt,-- thoihan_id, tg_thue_tu, tg_thue_den,songay_sd,
			--                    pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id ,
			--                    hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id, --pbh_db_id,
			--                    trangthaitb_id, doituong_id,doituong_ct_id, ma_tiepthi,ma_tiepthi_new,to_tt_id,donvi_tt_id,
			--                    donvi_tt,nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu,goi_id, tien_camket,                    
			--                    lydo_khongtinh_luong,manv_ptm,tennv_ptm, 
			--                    pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loainv_id,ten_loainv,loai_ld,manv_hotro,tyle_hotro,
			--                    ngay_tt,soseri,tien_tt, ht_tra_id, dthu_ps,dthu_goi_goc,dthu_goi,nop_du, 'ccq_202404',
			--                    sonha, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_id,  chuquan_id, tocdo_id
			--        from ttkd_bct.ccq_202404 a
			--        where ketqua like '%Duoc tinh ptm%'
			--                   and not exists (select thuebao_id from ttkd_bsc.ct_bsc_ptm where thang_ptm='202404' and nguon='ccq_202404' and thuebao_id=a.thuebao_id)
			--			    ;
			-- 
			-- commit;
 
 
-- Trong/ngoai CT: anh Nghia gui file chay dot 2 ---> sung dung file imp_ngoai_ctr
delete from ttkd_bsc.ct_bsc_ptm
	-- select * from ttkd_bsc.ct_bsc_ptm
    where thang_ptm='202404' and nguon='ptm_codinh_202404_bs -- Trong/Ngoai co che tinh luong'
    ;
    
insert into ttkd_bsc.ct_bsc_ptm 
                    (thang_luong, thang_ptm,ma_gd,ma_kh,ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
                    tien_dnhm,tien_sodep,thang_bddc,thang_ktdc,sothang_dc,ma_da,ma_duan_banhang,
                    ngay_bbbg, thoihan_id, tg_thue_tu, tg_thue_den,
                    pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
                    hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id,trangthaitb_id, 
                    doituong_id,doituong_ct_id, ma_tiepthi,ma_tiepthi_new,to_tt_id,donvi_tt_id,donvi_tt,
                    nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu,goi_id, tien_camket,
                    lydo_khongtinh_luong,manv_ptm,tennv_ptm,
                    pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loainv_id,ten_loainv,loai_ld,manv_hotro,tyle_hotro,
                    ngay_tt,soseri,tien_tt, dthu_ps, dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang, sl_mailing, nop_du,nguon,
                    so_nha, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_nhan_id, 
                    chuquan_id,heso_hotro_nvptm, heso_hotro_nvhotro, sohopdong, heso_dichvu, heso_diaban_tinhkhac)
        select  '1' thang_ins, '202404' thang_ptm, ma_gd, ma_kh, ma_tb,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,mst,mst_tt,
					'' tien_dnhm,'' tien_sodep,thang_bddc,thang_ktdc,sothang_dc, ma_duan,ma_duan_banhang,
					ngay_bbbg,thoihan_id, tg_thue_tu, tg_thue_den,
					pbh_nhan_id,pbh_nhan_goc_id,kieuhd_id,kieutn_id,kieuld_id,loaihd_id,dichvuvt_id,loaitb_id,
					hdkh_id, hdtb_id,khachhang_id,thanhtoan_id,thuebao_id,thuebao_cha_id, 
					trangthaitb_id, doituong_id,doituong_ct_id, ma_tiepthi,ma_tiepthi_new,to_tt_id,donvi_tt_id,
					donvi_tt,nhanviengt_id,ma_nguoigt,nguoi_gt,nhom_gt,ghi_chu,goi_id, '' tien_camket,
					lydo_khongtinh_luong,manv_ptm,tennv_ptm,
					pbh_ptm_id,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loainv_id,ten_loainv,loai_ld,manv_hotro,tyle_hotro,
					ngay_tt,soseri,tien_tt, dthu_ps,dthu_goi_goc,dthu_goi,dthu_goi_ngoaimang,sl_mailing, 1 nop_du, 'ptm_codinh_202404_bs -- Trong/Ngoai co che tinh luong',
					sonha, ap_id, khu_id, pho_id, phuong_id, quan_id, tinh_id, nhanvien_id, 
					chuquan_id,tyle_am, tyle_hotro, so_hop_dong, heso_dichvu, heso_diaban_tinhkhac
        from ttkd_bct.ptm_codinh_202404_bs a;
                    
    
commit;


-- Digishop (MyTV Mobile) ----anh Tuyen import
delete from ttkd_bsc.ct_bsc_ptm 
	--- select * from ttkd_bsc.ct_bsc_ptm
	where thang_ptm=202404 and dich_vu='MyTV MOBILE'
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
					   ,858 dongia, 202404 thang_tldg_dt, 202404 thang_tlkpi, 202404 thang_tlkpi_to, 202404 thang_tlkpi_phong        ---thang n
		from ttkd_bsc.digishop a, ttkd_bsc.nhanvien_202404 b
		where a.thang=202404
						and a.ma_gioithieu=b.ma_nv 
						and danhmuc='MyTV MOBILE' and dia_ban like 'TP H_ Ch_ Minh' and ma_gioithieu is not null 
						and trangthai_shop like 'Th_nh c_ng' and trangthai_doitac like 'Th_nh c_ng'
   ;
   commit;
    
---- MyTV OTT shop online: File do Ban KTNV gui, anh Phuong imp. Tu thang 03 da chuyen sang Digishop               
--delete from ct_bsc_ptm where thang_ptm=202404 and dich_vu='MyTV OTT'
--	;              
--insert into ct_bsc_ptm a
--        (thang_luong, thang_ptm, dich_vu, tenkieu_ld, ma_gd, ma_tb, ten_tb, sdt_lh, email_lh, diachi_ld, chuquan_id, goi_cuoc, ngay_bbbg,  so_gt, dthu_goi_goc, dthu_goi, 
--        diaban, ma_tiepthi, ma_tiepthi_new, doituong_kh, dichvuvt_id, loaitb_id, soseri, tien_tt, ngay_tt, dthu_ps, nguon
--	   )
--    select '202404', '202404', 'MyTV OTT', 'freedoo - mytv ott', magd, donhang, upper(tenkh), dt_kh, emailkh, diachi, 145,  goidk, ngaykh,  cmnd, tongdt, tongdt, 
--                loaidon, mahrm, mahrm, 'KHCN', 4,271, serial, tongdt, ngaydon, tongdt, 'dulieu_ftp.hcm_mytv_online_202404@vinadata'
--        from dulieu_ftp.hcm_mytv_online_202404@vinadata
--        where mahrm is not null and ma_tinh='HCM' and tinhban='HCM'                 
--    union all                
--    select '202404','202404', 'MyTV OTT', 'freedoo - mytv ott', magd, donhang, upper(tenkh), dt_kh, emailkh, diachi, 145, goidk, ngaykh, cmnd, thulao_nb, thulao_nb, 
--                loaidon, mahrm, mahrm, 'KHCN',  4, 271, serial, tongdt, ngaydon, tongdt, 'dulieu_ftp.hcm_mytv_online_202404@vinadata'
--        from dulieu_ftp.hcm_mytv_online_202404@vinadata
--        where mahrm is not null and ma_tinh<>'HCM' and tinhban='HCM'
--	   ;
                 
                 
update ttkd_bsc.ct_bsc_ptm a 
    set (manv_ptm,tennv_ptm, ma_to,ten_to,ma_pb,ten_pb,ma_vtcv, loainv_id, ten_loainv, loai_ld)
          = (select b.manv_hrm manv_ptm, b.ten_nv tennv_ptm,b.ma_to,b.ten_to,b.ma_pb,b.ten_pb,b.ma_vtcv,vtcv.loainv_id,vtcv.ten ten_loainv,b.loai_ld
                from ttkd_bsc.nhanvien_202404 b 
                        left join ttkd_bsc.dm_phongban pb on b.ma_pb=pb.ma_pb
                        left join (select distinct c.ma_vtcv, d.loainv_id, d.ten from ttkd_bsc.dm_vtcv c, ttkd_bsc.loai_nv d
                                            where c.loainv_id=d.loainv_id) vtcv on b.ma_vtcv=vtcv.ma_vtcv
                where b.manv_hrm = a.ma_tiepthi) 
    where thang_ptm=202404 and dich_vu='MyTV OTT'
                            
;
commit;


select ma_gd, ma_tb, loaitb_id
from ttkd_bsc.ct_bsc_ptm
where thang_ptm>='202301' and dichvuvt_id in (13,14,15,16)
group by ma_gd, ma_tb, loaitb_id
having count(*)>1;

