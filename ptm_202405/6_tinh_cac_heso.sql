--thang_luong = 5 --> ktra dthu ps thay doi toc do file 2
--thang_luong = 4 --> dung thu chuyen dung that file 5
--thang_luong = 3 --> he so goc nop tre file 6d
--thang_luong = 2 --> bo sung tra truoc file 6
--thang_luong = 1 --> cap nhat tbao ngan han file 6

--file nay chay 2 dot, full code
		/*Danh sach cac he so
		heso_dichvu, heso_dichvu_1, phanloai_kh, heso_khachhang, heso_tbnganhan, heso_tratruoc
		, heso_khuyenkhich, heso_kvdacthu, heso_vtcv_nvptm, heso_vtcv_dai, heso_vtcv_nvhotro
		, heso_hotro_nvptm, heso_hotro_dai, heso_hotro_nvhotro, heso_quydinh_nvptm, heso_quydinh_dai
		, heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_hoso, heso_dichvu_dnhm, dongia
		*/
-- Kiem tra luu bang:
select * from ttkd_bsc.ct_bsc_ptm_202404 where thang_ptm = 202405
;
create table ttkd_bsc.ct_bsc_ptm_202405_l1 as
	select * from ttkd_bsc.ct_bsc_ptm where thang_ptm >= 202305 		--thang n-24
;

-- Update lai thang_luong:
update ttkd_bsc.ct_bsc_ptm a 
    set thang_luong=thang_ptm
    where thang_luong<>thang_ptm
    ;


-- loaihinh_tb moi:
			insert into ttkd_bsc.dm_loaihinh_hsqd
					  (loaitb_id, loaihinh_tb,dichvuvt_id,ngay_cn) 
			    select loaitb_id, loaihinh_tb,dichvuvt_id,sysdate     
				   from css.loaihinh_tb@dataguard a
				   where sudung = 1 and not exists (select * from ttkd_bsc.dm_loaihinh_hsqd where loaitb_id=a.loaitb_id)
				   ;
				   commit;
				   rollback;
	   
	  -- select * from ttkd_bsc.dm_loaihinh_hsqd where ngay_cn is not null order by ngay_cn desc;
        
-- Tien thanh toan hop dong dau vao:  (thang 6 ktra lai quet tren DATAGURAD day du, TTKDDB do dong bo
		select * from hocnq_ttkd.temp_tt
					where ma_gd in ('HCM-LD/01479990',
													'HCM-LD/01424347',
													'HCM-LD/01477643')
				;
		
		drop table ttkd_hcm.temp_tt purge
		;
		
		---chay tren DataGuard
		create table ttkd_hcm.temp_tt as
--		insert into ttkd_hcm.temp_tt
				select kh.loaihd_id, a.ma_gd, a.hdkh_id, b.hdtb_id,a.ngay_cn, a.ngay_tt ngay_tt, a.soseri, sum(b.tien) tien, a.trangthai trangthai--, a.ht_tra_id
				    from css.v_hd_khachhang kh
								, css.v_hd_thuebao tb
							  , css.v_phieutt_hd a
							 , css.v_ct_phieutt b
				    where kh.hdkh_id = tb.hdkh_id and tb.hdtb_id = b.hdtb_id and a.phieutt_id = b.phieutt_id and a.trangthai = 1 --and b.tien >0 
										and tb.tthd_id = 6
								and to_number(to_char(a.ngay_cn,'yyyymm'))>=202402					--thang n -3
--								and tb.hdtb_id = 24531031
--								and a.hdkh_id not in (select hdkh_id from ttkd_hcm.temp_tt)
				group by kh.loaihd_id, a.ma_gd, a.hdkh_id, b.hdtb_id,a.ngay_cn, a.ngay_tt, a.soseri, a.trangthai--, a.ht_tra_id
						;
		---move TTKDDB				
			drop table hocnq_ttkd.temp_tt purge;
			create table hocnq_ttkd.temp_tt as
			select * from ttkd_hcm.temp_tt@dataguard
			;
			create index hocnq_ttkd.temp_tt_hdtbid on hocnq_ttkd.temp_tt (hdtb_id)
			;
			select * from hocnq_ttkd.temp_tt;
		
		
			MERGE INTO ttkd_bsc.ct_bsc_ptm a
			USING (select ngay_tt, soseri, tien, trangthai, hdtb_id--, case when tien > 0 then ht_tra_id else null end ht_tra_id 
							from hocnq_ttkd.temp_tt) b
			ON (b.hdtb_id = a.hdtb_id)
			WHEN MATCHED THEN
					update SET ngay_tt = b.ngay_tt, soseri = b.soseri , tien_tt = b.tien, trangthai_tt_id = b.trangthai--, ht_tra_id = b.ht_tra_id
---			select ma_tb, thuebao_id, ngay_tt, soseri, tien_tt, trangthai_tt_id, hdtb_id from ttkd_bsc.ct_bsc_ptm a
			WHERE thang_ptm >= 202402 and chuquan_id <> 264 and thang_tldg_dt is null and nvl(trangthai_tt_id,0)=0			--thang n -3
								and loaitb_id not in (20, 21) and ma_tb = 'hcm_hddt_00010808'
								and exists(select 1 from hocnq_ttkd.temp_tt where hdtb_id = a.hdtb_id)
--								and ma_gd in ('HCM-LD/01479990',
--'HCM-LD/01424347',
--'HCM-LD/01477643')
				--	and nvl(trangthai_tt_id,0)=0
			;
			
commit;
rollback;
                                                                     
-- DTHU_PS:
-- Dot 1: da update ps 1 lan 4/6/25
		drop table ttkd_bsc.tmp_ct_no purge;
		;
	select * from ttkd_bsc.tmp_ct_no;
		create table ttkd_bsc.tmp_ct_no as 
				select thuebao_id, khoanmuctt_id, nogoc 
						from bcss.v_ct_no@dataguard 
						where phanvung_id=28 and ky_cuoc=20240501
					;
		create index idx_ctno__tbid on ttkd_bsc.tmp_ct_no (thuebao_id);
   
-- dthu_ps thang n:
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps = (select sum(nogoc) from ttkd_bsc.tmp_ct_no
								    where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
		    where thang_ptm=202405 and loaitb_id<>21 and dthu_ps is null 
			   and exists (select 1 from ttkd_bsc.tmp_ct_no
								where nogoc>0 and khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
		;
     
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps_n1 = (select sum(nogoc) from ttkd_bsc.tmp_ct_no
										 where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
		    where thang_ptm=202404 and loaitb_id not in (20,21,131)
			   and dthu_ps_n1 is null
			   and exists (select 1 from ttkd_bsc.tmp_ct_no
							   where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
					   ;
            
			update ttkd_bsc.ct_bsc_ptm a 
				   set dthu_ps_n2 = (select sum(nogoc) from ttkd_bsc.tmp_ct_no
											 where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
			    where thang_ptm=202403 and loaitb_id not in (20,21,131)
				   and dthu_ps_n2 is null
				   and exists (select 1 from ttkd_bsc.tmp_ct_no
				   where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
			;

                            
			update ttkd_bsc.ct_bsc_ptm a 
				   set dthu_ps_n3 = (select sum(nogoc) from ttkd_bsc.tmp_ct_no
											 where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
			    where thang_ptm=202402 and loaitb_id not in (20,21,131)
				   and dthu_ps_n3 is null
				   and exists (select 1 from ttkd_bsc.tmp_ct_no
								   where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id);          
commit;
                       
-- Dot 2: 
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps = (select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd where ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
		    -- select * from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm=202405 and loaitb_id<>21 and nvl(dthu_ps, 0) = 0 
			   and exists(select 1 from ttkd_bct.cuoc_thuebao_ttkd where dthu>0 and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
             ;   
                            
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps_n1 = (select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd where ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
--		    select * from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm=202404 and loaitb_id<>21 and nvl(dthu_ps_n1, 0) = 0
			   and exists(select 1 from ttkd_bct.cuoc_thuebao_ttkd where dthu>0 and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
			   ;
                
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps_n2 = (select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd where ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
--		    select * from ttkd_bsc.ct_bsc_ptm a		    
		    where thang_ptm=202403 and loaitb_id<>21 and nvl(dthu_ps_n2, 0) = 0
			   and exists(select 1 from ttkd_bct.cuoc_thuebao_ttkd where dthu>0 and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
			   ;
                            
			update ttkd_bsc.ct_bsc_ptm a 
				   set dthu_ps_n3 = (select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd where ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
--		    select * from ttkd_bsc.ct_bsc_ptm a		    
			    where thang_ptm=202402 and loaitb_id<>21 and nvl(dthu_ps_n3, 0) = 0
				   and exists(select 1 from ttkd_bct.cuoc_thuebao_ttkd where dthu>0 and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
				   ;
commit;

-- DTHU GOI TB NGAN HAN THANG n:
		update ttkd_bsc.ct_bsc_ptm a 
			   set  ngay_cat = (select ngay_cat from css_hcm.db_thuebao where thuebao_id=a.thuebao_id)
					 , trangthaitb_id = (select trangthaitb_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id)
					 , songay_sd = (select (case when db.trangthaitb_id in (5,6) and to_char(db.ngay_td,'yyyymm')>=to_char(db.ngay_sd,'yyyymm') then trunc(db.ngay_td)-trunc(db.ngay_sd)+1
																	  when db.trangthaitb_id in (7) and to_char(db.ngay_cat,'yyyymm')>=to_char(db.ngay_sd,'yyyymm') then trunc(db.ngay_cat)-trunc(db.ngay_sd)+1
																	  when db.trangthaitb_id not in (5,6,7) and thoihan_id=1 then trunc(db.tg_thue_den)-trunc(db.ngay_sd)+1
														  else null end)
												from css_hcm.db_thuebao db
												where db.thuebao_id=a.thuebao_id
											)
					 , dthu_goi = nvl(a.dthu_ps,0) + nvl(a.dthu_ps_n1,0) + nvl(a.dthu_ps_n2,0) + nvl(a.dthu_ps_n3,0)
					 , thang_luong = 1                                      
		    -- select ma_tb, trangthaitb_id, ngay_bbbg, ngay_cat, (select ngay_cat from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) ngay_cat_dbonline, dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3, dthu_goi, nvl(a.dthu_ps,0)+nvl(a.dthu_ps_n1,0)+nvl(a.dthu_ps_n2,0)+nvl(a.dthu_ps_n3,0) dthu_goi_new, ghi_chu from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm >= 202402 and thoihan_id=1 and dichvuvt_id in (1, 10, 11, 4, 7, 8, 9) ---thang n-3
					   and (thang_tldg_dt is null or thang_tldg_dt = 202405)											---thang n
					   and nvl(a.dthu_ps,0)+nvl(a.dthu_ps_n1,0)+nvl(a.dthu_ps_n2,0)+nvl(a.dthu_ps_n3,0) > nvl(dthu_goi,0)
					   and exists(select 1 from css_hcm.db_thuebao 
												where trangthaitb_id=7 and to_number(to_char(ngay_cat,'yyyymm')) = 202405 --thang n
															and thuebao_id = a.thuebao_id
										)  
			   ;      
            commit;
  
-- Tinh lai dthu goi cho CA, IVAN, HDDT, TAX, VNPT HKD chua co dthu_goi tai thang_ptm:
			----Tbao DTHU_goi = 0, nhung tong dthu_ps >0 --> UPDATE DATCOC_CSD into DTHU_GOI
			----Ap dung dich vu thang tinh 1 lan
				drop table hocnq_ttkd.temp_ps purge
				;
				select * from hocnq_ttkd.temp_ps
				;
				create table hocnq_ttkd.temp_ps as
--insert into hocnq_ttkd.temp_ps
								select thang_ptm, thuebao_id, ma_gd, ma_tb, dthu_goi, dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3
										  , nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0) dthu_tong, datcoc_csd, sothang_dc, thang_bddc
										  , (select sum(nogoc) from ttkd_bsc.tmp_ct_no
											 where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953)
													   and thuebao_id=a.thuebao_id) dthu_ps_n
										  ,(select round(cuoc_dc/1.1,0)
													from css_hcm.db_datcoc 
													where hieuluc=1 and ttdc_id = 0 and to_number(to_char(ngay_cn,'yyyymm'))>=202402 and thuebao_id=a.thuebao_id
												) datcoc_csd_new
										  ,(select (months_between( to_date(least(thang_kt,nvl(thang_kt_dc,'999999'),nvl(thang_huy,'999999')) , 'yyyymm') , 
																					    to_date(thang_bd,'yyyymm'))+1)
												 from css_hcm.db_datcoc where hieuluc=1 and ttdc_id = 0 and to_char(ngay_cn,'yyyymm') >= 202402 and thuebao_id=a.thuebao_id
												) sothang_dc_new
										 ,(select thang_bd from css_hcm.db_datcoc 
												where hieuluc=1 and ttdc_id = 0 and to_number(to_char(ngay_cn,'yyyymm'))>= 202402 and thuebao_id=a.thuebao_id
											) thang_bddc_new
										 ,thang_tldg_dt, thang_tlkpi_to, thang_tlkpi_phong, lydo_khongtinh_dongia
								from ttkd_bsc.ct_bsc_ptm a
								where thang_ptm >= 202402 and nvl(dthu_goi, 0) = 0 --dthu_goi is null		---thang n -3
										  and (thang_tldg_dt is null or thang_tldg_dt = 202405)											---thang n
										  and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302)
				--						  and (select 1 from ttkd_bsc.ct_no_20240401
				--														 where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953)
				--															   and thuebao_id=a.thuebao_id) >0
										  and exists(select 1 from css_hcm.db_datcoc where cuoc_dc>0 and hieuluc=1  and ttdc_id = 0 and thuebao_id=a.thuebao_id)
--										  and ma_tb = 'hcm_ca_00067906'
				;
				commit;
      
      
			update ttkd_bsc.ct_bsc_ptm a 
							set   thang_luong='2' , 
								   dthu_goi_goc = (select datcoc_csd_new from hocnq_ttkd.temp_ps where --dthu_tong>0 and 
																				ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm),
								   dthu_goi = (select datcoc_csd_new from hocnq_ttkd.temp_ps where --dthu_tong>0 and 
								   											ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm), 
								   datcoc_csd= (select datcoc_csd_new from hocnq_ttkd.temp_ps where --dthu_tong>0 and 
																			ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm),
								   thang_bddc= (select thang_bddc_new from hocnq_ttkd.temp_ps where --dthu_tong>0 and 
																			ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm), 
								   sothang_dc= (select sothang_dc_new from hocnq_ttkd.temp_ps where --dthu_tong>0 and 
																			ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm), 
								   thang_tldg_dt='', thang_tlkpi='', thang_tlkpi_to='', thang_tlkpi_phong=''      
				-- select thang_luong, thang_ptm, hdkh_id, thuebao_id, ma_tb,  dich_vu, tenkieu_ld, dthu_goi_goc, dthu_goi, thang_ptm, dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3 , lydo_khongtinh_dongia, doanhthu_dongia_nvptm, luong_dongia_nvptm, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to from ttkd_bsc.ct_bsc_ptm a
				where 
							exists  (select dthu_tong from hocnq_ttkd.temp_ps where --dthu_tong>0 and 
														ma_gd=a.ma_gd and ma_tb =a.ma_tb and thang_ptm=a.thang_ptm) 
							and 
							nvl(dthu_goi, 0) = 0 --dthu_goi is null
--						and ma_tb = 'hcm_ca_00067906'
;
commit;
rollback;

				select thang_luong, thuebao_id, ma_tb,  dich_vu, tenkieu_ld, datcoc_csd, dthu_goi_goc, dthu_goi, thang_ptm, dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3
							, lydo_khongtinh_dongia, doanhthu_dongia_nvptm, luong_dongia_nvptm, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to 
				from ttkd_bsc.ct_bsc_ptm a
					where thang_luong='2' 
				;
     
/* Quy dinh hien hanh ko con su dung 2 truong nay
-- ma_dt_kh, pbh_ql_id
update ct_bsc_ptm a 
        set (ma_dt_kh, pbh_ql_id) = (select ma_dt_kh,pbh_ql_id from ttkd_bct.db_thuebao_ttkd where thuebao_id=a.thuebao_id and dichvuvt_id=a.dichvuvt_id)
    where thang_ptm=202403 and pbh_ql_id is null
        and (lydo_khongtinh_luong not like '%Chu quan khong thuoc TTKD-HCM%' or lydo_khongtinh_luong is null)
        and loaitb_id not in (20,21,149) 
        and exists(select 1 from ttkd_bct.db_thuebao_ttkd where thuebao_id=a.thuebao_id and dichvuvt_id=a.dichvuvt_id);
 */    

-- SMS Brandname: Tinh lai dthu goi, dthu_ps thang n va dthu_ps thang n+1
		drop table ttkd_bsc.tmp_bsc_smsbrn purge
		;
		select * from ttkd_bsc.tmp_bsc_smsbrn;
		create table ttkd_bsc.tmp_bsc_smsbrn as
		    select thang_ptm, ma_gd, ma_tb, thuebao_id, nguon
						, dthu_ps, dthu_ps_n1
						 , dthu_goi dthu_goi_n, dthu_goi_ngoaimang dthu_goi_ngoaimang_n
						 , cast(null as number(12)) dthu_goi_n1, cast(null as number(12)) dthu_goi_ngoaimang_n1
						 , cast(null as number(12)) dthu_goi_bq, cast(null as number(12)) dthu_goi_ngoaimang_bq            
		    from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202404 and loaitb_id=131 ---thang n -1
		    ;


--update bsc_smsbrn_202403 a 
--    set dthu_goi_ngoaimang_n = (select sum(nogoc) from ttkd_bsc.v_tonghop  
--                                                            where ky_cuoc=20240201 and khoanmuctc_id in (37,38,39,40,935,936,938,939,943,944,945,4097) and ma_tb=a.ma_tb)
--        ,dthu_goi_ngoaimang_n1= (select sum(nogoc) from ttkd_bsc.v_tonghop  
--                                                            where ky_cuoc=20240301 and khoanmuctc_id in (37,38,39,40,935,936,938,939,943,944,945,4097) and ma_tb=a.ma_tb);
		update ttkd_bsc.tmp_bsc_smsbrn a 
			    set dthu_goi_ngoaimang_n = (select sum(nogoc) from bcss.v_tonghop@dataguard  --thang n- 1
															where phanvung_id=28 and ky_cuoc=20240401 and khoanmuctc_id in (37,38,39,40,935,936,938,939,943,944,945,4097) and ma_tb=a.ma_tb)
				   ,dthu_goi_ngoaimang_n1= (select sum(nogoc) from bcss.v_tonghop@dataguard    ---thang n
                                                            where phanvung_id=28 and ky_cuoc=20240501 and khoanmuctc_id in (37,38,39,40,935,936,938,939,943,944,945,4097) and ma_tb=a.ma_tb)
			;
             
			update ttkd_bsc.tmp_bsc_smsbrn a 
			    set   dthu_goi_n = nvl(dthu_ps,0)-nvl(dthu_goi_ngoaimang_n,0)
					  , dthu_goi_n1 = nvl(dthu_ps_n1,0)-nvl(dthu_goi_ngoaimang_n1,0)
			;


			update ttkd_bsc.tmp_bsc_smsbrn a 
			    set  dthu_goi_bq = round( (nvl(dthu_goi_n,0)+nvl(dthu_goi_n1,0) ) /2,0),
					  dthu_goi_ngoaimang_bq = round( (nvl(dthu_goi_ngoaimang_n,0)+nvl(dthu_goi_ngoaimang_n1,0) )/2,0)
					;

                               
			update ttkd_bsc.ct_bsc_ptm a 
			    set (dthu_goi_goc, dthu_goi, dthu_goi_ngoaimang, thang_luong)=
					  (select nvl(dthu_goi_bq,0)+nvl(dthu_goi_ngoaimang_bq,0) , dthu_goi_bq, dthu_goi_ngoaimang_bq, 202405		--thang n
						 from ttkd_bsc.tmp_bsc_smsbrn
						 where thuebao_id=a.thuebao_id)
			    where thang_ptm=202404 and loaitb_id=131  --thang n-1
			    ;
            commit;
            rollback;

-- Doi tuong KH:   chay cac th doituong_kh = null xy ly tiep                      
		update ttkd_bsc.ct_bsc_ptm 
						set doituong_kh=null 
	--		select * from ttkd_bsc.ct_bsc_ptm
		    where thang_ptm = 202405 and (dichvuvt_id!=2 or (dichvuvt_id=2 and loaitb_id<>21) ) and ma_kh<>'GTGT rieng' and doituong_kh is not null
		    ;

			update ttkd_bsc.ct_bsc_ptm a 
				   set doituong_kh = (case when (select c.khdn from css_hcm.db_khachhang b, css_hcm.loai_kh c 
															    where b.loaikh_id=c.loaikh_id and khachhang_id=a.khachhang_id) =0 then 'KHCN' else 'KHDN' end)
			    --	    select * from ttkd_bsc.ct_bsc_ptm a
			    where (thang_ptm = 202405 --- thang n
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
					 and ma_kh <> 'GTGT rieng'  and doituong_kh is null 
				   and (dichvuvt_id !=2 or (dichvuvt_id=2 and loaitb_id<>21) ) 
				 
				   ;
                        
              
    -- Update doi voi ds tbao VNPts, hoac ktra cac th ngoai le                  
				update ttkd_bsc.ct_bsc_ptm a
					   set doituong_kh = (case when (loaitb_id not in (20,149) and doituong_id in (374, 387, 361, 362))
																    or (loaitb_id in (20,149) and doituong_id in (1,25))
													   then 'KHCN' else 'KHDN' end)
			--	    select * from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm = 202405 and ma_kh <> 'GTGT rieng' and doituong_kh is null 
					   and (lydo_khongtinh_luong is null or lydo_khongtinh_luong not like '%Chu quan khong thuoc TTKD-HCM%')
					   ;
                            
              --Update doituong KHDN doi voi ds ID447                              
				update ttkd_bsc.ct_bsc_ptm a 
					   set doituong_kh ='KHDN'            
			--	    select * from ttkd_bsc.ct_bsc_ptm a
				    where  thang_ptm=202405 and ma_kh='GTGT rieng' and doituong_kh is null
				    ;
             
		   ---Update cac th dau vao sai, update theo ten_tb
               update ttkd_bsc.ct_bsc_ptm a 
					set doituong_kh='KHDN'
					    -- select chuquan_id, khachhang_id, ma_tb, ten_tb, doituong_kh, dichvuvt_id from ttkd_bsc.ct_bsc_ptm a
					    where thang_ptm = 202405 and doituong_kh = 'KHCN' 
						   and (bo_dau(upper(ten_tb)) like 'CONG TY%' or bo_dau(upper(ten_tb)) like 'CTY%'  
								 OR bo_dau(upper(ten_tb)) like 'CN TONG CONG TY%' or bo_dau(upper(ten_tb)) like 'CHI NHANH%'
								 OR bo_dau(upper(ten_tb)) like 'NGAN HANG%' or bo_dau(upper(ten_tb)) like 'CHI CUC %' 
								 OR bo_dau(upper(ten_tb)) like 'VPDD%' OR bo_dau(upper(ten_tb)) like 'VAN PHONG DAI DIEN%'
								 OR bo_dau(upper(ten_tb)) like '%TR__NG PH_ TH_NG TRUNG H_C%'
								 OR bo_dau(upper(ten_tb)) like '%TR__NG TRUNG H_C PH_ TH_NG%' ) 
			 ;

commit;
rollback;

-- QLDA: xoa , chay lai
delete from ttkd_bsc.ct_bsc_ptm_kiemtraduan where thang=202405 and  ma_gd = 'HCM-TD/00690780';
		;
		select * from ttkd_bsc.ct_bsc_ptm_kiemtraduan where thang=202405 and ma_gd = 'HCM-TD/00690780';

			insert into ttkd_bsc.ct_bsc_ptm_kiemtraduan
								(thang, ptm_id, thang_ptm, ma_gd, ma_tb, dich_vu, ma_duan_banhang, mst, mst_tt
								, dichvuvt_id, loaitb_id, duan_id, duan_daduyet, duan_mst, kt_loaitb_id, kt_mst, kt_nghiemthu
								)
								
			    select to_char(trunc(sysdate, 'month') - 1, 'yyyymm') thang, a.id, a.thang_ptm, a.ma_gd, a.ma_tb, a.dich_vu, a.ma_duan_banhang, a.mst, a.mst_tt, a.dichvuvt_id, a.loaitb_id, b.ma_yeucau, b.daduyet, b.masothue
						 , (case when exists (select 1 from ttkdhcm_ktnv.amas_yeucau_dichvu c, ttkdhcm_ktnv.amas_loaihinh_tb d
																  where c.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', '')) 
																				---and MA_HIENTRANG = 12 		---cho ktra duyet TTKD vb
																				and c.ma_dichvu=d.loaitb_id and d.loaitb_id_obss= a.loaitb_id
																	) 
														then 1 
										when exists(select 1 from ttkd_bsc.map_loaihinhtb where loaitb_id=a.loaitb_id and loaitb_id_qlda=b.ma_dichvu) 
											    then 1 
										else 0 end 
							  ) kt_loaitb_id
						 , (case when regexp_replace (mst, '\D', '')=regexp_replace (b.masothue, '\D', '') or regexp_replace (mst_tt, '\D', '')=regexp_replace (b.masothue, '\D', '') 
							    then 1 else 0 end ) kt_mst
						 , (case when exists (select 1 from ttkdhcm_ktnv.amas_yeucau_dichvu c, ttkdhcm_ktnv.amas_loaihinh_tb d
																  where c.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', '')) 
																				and MA_HIENTRANG = 12 
																				and c.ma_dichvu=d.loaitb_id and d.loaitb_id_obss= a.loaitb_id
																	) 
														then 1 
									 when exists (select 1 from ttkdhcm_ktnv.amas_yeucau_dichvu c, ttkdhcm_ktnv.amas_loaihinh_tb d
																  where MA_HIENTRANG <> 12 and c.ma_dichvu=d.loaitb_id and d.loaitb_id_obss= a.loaitb_id
																			and	c.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', '')) 
																	) 
														then 0
									else null
							    end ) kt_nghiemthu
			    from ttkd_bsc.ct_bsc_ptm a, ttkdhcm_ktnv.amas_yeucau b
			    where (a.thang_ptm >= 202402 --- thang n - 3
								or a.thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
						and nvl(thang_tldg_dt, 999999) >= 202405 and nvl(thang_tlkpi_phong, 999999) >= 202405  --lay cac tbao chua chi dektra duan
						and loaihd_id<>31 and dichvuvt_id <> 2--thuebao_id is not null 
						and a.doituong_kh='KHDN' and nhom_tiepthi<>2
						and  to_number(regexp_replace (a.ma_duan_banhang, '\D', '')) = b.ma_yeucau (+)
--						and a.ma_duan_banhang= '235405'
				
				
				; 
				commit;
        
        
-- ket qua kiem tra du an: neu khong hop le thi giam dthu quy doi 50%
			update ttkd_bsc.ct_bsc_ptm_kiemtraduan a
			set kiemtra_duan = (
													with kq as (
															  select ptm_id, ma_duan_banhang, duan_id, duan_daduyet, kt_mst, kt_loaitb_id
																		   , case when ma_duan_banhang is null then '; Ho so khong nhap ma du an' end kq1
																		   , case when ma_duan_banhang is not null and duan_id is null then '; Ma du an '||ma_duan_banhang||' chua dang ky tren QLDA' end kq2
--																		   , case when duan_id is not null and kt_nghiemthu = 0 then '; Ma du an '||ma_duan_banhang||' chua nghiem thu tren QLDA' end kq3			---cho vb trien khai TTKD
																		   , case when duan_id is not null and (duan_daduyet is null or duan_daduyet <> 1) then '; Du an chua duoc duyet' end kq4
																		   , case when duan_id is not null and kt_mst=0 then '; Khong dung KH (mst)' end kq5
																		   , case when duan_id is not null and nvl(kt_loaitb_id, 0)=0 then '; Khong dung dich vu dang ky' end kq6
															  from ttkd_bsc.ct_bsc_ptm_kiemtraduan
															  where thang = to_char(trunc(sysdate, 'month') - 1, 'yyyymm')
															)                                        
											  select nvl(c.kq1,'') || nvl(c.kq2,'')-- || nvl(c.kq3,'') 
															|| nvl(c.kq4,'') || nvl(c.kq5,'') || nvl(c.kq6,'')--, ma_duan_banhang
											  from  kq c
											  where c.ptm_id = a.ptm_id
										) 
			where thang = 202405 	
--						and ma_duan_banhang = '235405'
			;


			update ttkd_bsc.ct_bsc_ptm a 
				   set kiemtra_duan = '' 
		--- select ma_tb, kiemtra_duan from ttkd_bsc.ct_bsc_ptm a 
			    where thang_ptm>=202402 and doituong_kh='KHDN' and kiemtra_duan is not null
			 
			    ;
    
			update ttkd_bsc.ct_bsc_ptm a 
				   set kiemtra_duan = (select nvl(kt_nghiemthu, 0) || kiemtra_duan from ttkd_bsc.ct_bsc_ptm_kiemtraduan 
																							where thang = 202405 and ptm_id=a.id)
																
--		 select thang_luong, thang_ptm, kiemtra_duan, thang_tldg_dt, thang_tlkpi, nguon, ma_duan_banhang from ttkd_bsc.ct_bsc_ptm a 
			    where exists(select 1 from ttkd_bsc.ct_bsc_ptm_kiemtraduan where thang = 202405 and ptm_id=a.id)
--			    and ma_duan_banhang = '235405'
						
			    ;

		commit;
		rollback;

/* 
select * from ct_bsc_ptm a
    where a.thang_ptm=202403 and loaitb_id not in (20,21,210) 
        and duan_id is not null and kiemtra_duan is not null;
*/   

          
-- KH hien huu - KH moi:   
			---dot 2  khong xoa
			update ttkd_bsc.ct_bsc_ptm set khhh_khm='' 		
	--   select khhh_khm from ttkd_bsc.ct_bsc_ptm
			    where thang_ptm=202405 and khhh_khm is not null
			    ;
			    
                  --chay lai                          
			update ttkd_bsc.ct_bsc_ptm set khhh_khm='KHM' 
		--   select khhh_khm, dichvuvt_id from ttkd_bsc.ct_bsc_ptm
			    where (thang_ptm = 202405 --- thang n
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that					
							and khhh_khm is null 
							and (nguon='web Digishop' 
										 or (doituong_kh='KHCN'
											    and (dichvuvt_id!=2 or dichvuvt_id is null)
											    and nguon in ('ptm_codinh','ccq','dt_ptm_vnp','shop_hcm_mytv_online')
											)
									)
						   
				   ; 
  

			update ttkd_bsc.ct_bsc_ptm a 
			    set khhh_khm='KHHH'
		--   select khhh_khm, nguon, tenkieu_ld from ttkd_bsc.ct_bsc_ptm
			    where (thang_ptm = 202405 --- thang n
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
					--and khhh_khm is null 
				   and (nguon in ('thaydoitocdo','tailap') or loaihd_id<>1 ) 
				   ;
     ----chua chay trong dot 1              
				---chay hoi lau ---toi uu lai
			update ttkd_bsc.ct_bsc_ptm a 
				   set khhh_khm = 'KHHH'
		 -- select khhh_khm, loaitb_id, ma_duan_banhang from ttkd_bsc.ct_bsc_ptm a
			    where (thang_ptm = 202405 --- thang n
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
					and doituong_kh='KHDN' and mst is not null --thang n
					  and exists (select mst_chuan from ttkd_bct.db_thuebao_ttkd
												   where ma_dt_kh='dn' and cvnv is null and tb_dacbiet is null 
																  and trangthaitb_id not in (7,8,9) and to_number(to_char(ngay_sd,'yyyymm')) < 202405			--- thang n
																  and to_number(regexp_replace (mst, '\D', ''))=to_number(regexp_replace (a.mst, '\D', ''))
											)
										--	and thang_luong in (4)
--					  and ma_tb = '84916665480'
					  ;
                                              
					update ttkd_bsc.ct_bsc_ptm a 
					   set khhh_khm =  'KHHH'
					     -- select khhh_khm from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202405 and doituong_kh='KHDN'
						  and khhh_khm is null and nvl(loaitb_id, 21) !=21
				
				---hoac 1
						and   exists (select 1 from ttkd_bct.db_thuebao_ttkd
															   where ma_dt_kh='dn' and cvnv is null and tb_dacbiet is null                                                                         
																  and trangthaitb_id not in (7,8,9) and to_number(to_char(ngay_sd,'yyyymm')) < 202405		--thang n
																  and lower(so_gt)=lower(a.so_gt)
												  )
			---hoac 2
--						and  exists (select 1 from ttkd_bct.db_thuebao_ttkd
--										   where ma_dt_kh='dn' and cvnv is null and tb_dacbiet is null 
--											  and trangthaitb_id not in (7,8,9) and to_number(to_char(ngay_sd,'yyyymm')) < 202405		--thang n
--											  and khachhang_id=a.khachhang_id
--											  )          
					;
					
					update ttkd_bsc.ct_bsc_ptm a 
					   set khhh_khm =  'KHM'
					     -- select * from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm = 202405		--thang n
									and doituong_kh = 'KHDN' --and mst is not null
								  and khhh_khm is null and nvl(loaitb_id, 21) !=21    
					;
					
					select * from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm = 202405		--thang n
--									and doituong_kh = 'KHDN' and mst is not null
								  and khhh_khm is null and nvl(loaitb_id, 21) !=21 
								  ;
				commit;
       ---chua chay
       
    
    /*                   
         select khhh_khm, count(*) from ct_bsc_ptm a 
            where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') and doituong_kh='KHDN'
                and (lydo_khongtinh_luong is null or lydo_khongtinh_luong not like '%Chu quan khong thuoc TTKD-HCM%')
            group by khhh_khm;
     */               
 
                    
-- Dia ban:        vi tri ban hang truc tiep moi xet trong/ngoai dia ban
			update ttkd_bsc.ct_bsc_ptm a 
						set diaban =null
	---	select diaban from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202405 and (loaitb_id<>21 or ma_kh='GTGT rieng') and diaban is not null
			    ;
          
				update  ttkd_bsc.ct_bsc_ptm a
				    set diaban = (case 
--											when nhom_tiepthi=2 or thuebao_id is null then 'Khong xet trong/ngoai CT ban hang'
											when nhom_tiepthi=2 then 'Khong xet trong/ngoai CT ban hang'
--											when loaitb_id = 149 then 'Khong xet trong/ngoai CT ban hang'
											  when ma_vtcv not in ('VNP-HNHCM_BHKV_6','VNP-HNHCM_BHKV_6.1',
																		    'VNP-HNHCM_BHKV_17','VNP-HNHCM_BHKV_42','VNP-HNHCM_KHDN_3','VNP-HNHCM_KHDN_3.1',
																		    'VNP-HNHCM_KHDN_4','VNP-HNHCM_BHKV_41',
																		    'VNP-HNHCM_KHDN_18', 'VNP-HNHCM_KTNV_8') then 'Khong xet trong/ngoai CT ban hang'
--														ma_vtcv not in ('VNP-HNHCM_BHKV_6','VNP-HNHCM_BHKV_6.1',
--																		    'VNP-HNHCM_BHKV_17','VNP-HNHCM_BHKV_42','VNP-HNHCM_KHDN_3','VNP-HNHCM_KHDN_3.1',
--																		    'VNP-HNHCM_KHDN_4','VNP-HNHCM_BHKV_41',
--																		    'VNP-HNHCM_KHDN_18'
--																		    , 'VNP-HNHCM_BHKV_28', 'VNP-HNHCM_BHKV_27', 'VNP-HNHCM_KDOL_4', 'VNP-HNHCM_KDOL_5', 'VNP-HNHCM_KDOL_17', 'VNP-HNHCM_KDOL_3', 'VNP-HNHCM_KDOL_6', 'VNP-HNHCM_BHKV_51' ) 
											  when doituong_kh = 'KHCN' then 'Trong CT ban hang'
--											  when doituong_kh = 'KHDN' and subtr(kiemtra_duan, 2) = '1' then 'Trong CT ban hang'			---cho vb trien khai TTKD
											  when doituong_kh = 'KHDN' and substr(kiemtra_duan, 2) is null then 'Trong CT ban hang'
											  when doituong_kh = 'KHDN' and kiemtra_duan is not null then 'Ngoai CT ban hang'
											  else null end)
--							select thuebao_id, ma_tb, dichvuvt_id, loaitb_id, diaban, nhom_tiepthi, ma_vtcv, doituong_kh, kiemtra_duan, ma_duan_banhang, nguon from ttkd_bsc.ct_bsc_ptm a
				    where (a.thang_ptm >= 202402 --- thang n -3
								or a.thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
						and (loaitb_id<>21 or ma_kh='GTGT rieng')
					   and ma_pb is not null-- and diaban is null --and id_447 is not null
					   
					   and nvl(thang_tldg_dt, 999999) >= 202405 and nvl(thang_tlkpi_phong, 999999) >= 202405  --lay cac tbao chua chi dektra duan
					   --- select kiemtra_duan, diaban from ttkd_bsc.ct_bsc_ptm a 
					   	--and a.ma_gd = '00798153'
--						and a.thang_luong in (26) 
--						and ma_duan_banhang in ('235405')
					   ;

commit;
rollback;
-- Xet mien HS goc doi voi hop dong chuyen doi, nang cap, va lap moi cdbr/tsl co tra truoc      
			update ttkd_bsc.ct_bsc_ptm set mien_hsgoc='' 
		-- select * from ttkd_bsc.ct_bsc_ptm
			    where thang_ptm = 202405 and dichvuvt_id not in (2,13,14,15,16) and loaitb_id not in (21,172) and mien_hsgoc is not null
			    ;
                               
			update ttkd_bsc.ct_bsc_ptm a 
				   set mien_hsgoc = 1 
		-- select * from ttkd_bsc.ct_bsc_ptm
			    where (thang_ptm = 202405 --- thang n
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
					and mien_hsgoc is null 
				   and ( (dichvuvt_id not in (2,13,14,15,16) and (loaihd_id is null or chuquan_id in (266,264))  ---Newlife, VTH1 hoac hok co hop dong --> mien hso goc
								)
									or (	(thang_ptm = 202405 --- thang n
															or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
												and dichvuvt_id=4 and sothang_dc>=6)                                   -- Fiber, MyTV dong truoc truoc >=6 thang  
									or ( thang_ptm = 202405 and loaitb_id in (15,17) and thuebao_cha_id is not null)       -- isdn, thue bao luong (so con)
									or nguon not in ('ptm_codinh','ccq','dt_ptm_vnp') 
						)
							;
                  commit;
 
---da xong 15/06/24 19g
-- He so dich vu:    
		update ttkd_bsc.ct_bsc_ptm a 
		    set heso_dichvu = case when loaitb_id=61
                                                then
                                                    case when loaihd_id=1 and goi_id is null then 1.1                               -- MyTV ko tham gia goi tich hop => 1.1
                                                             when loaihd_id<>1 and kieuld_id=96 then 0.3                             -- MyTV tai lap
                                                        else 1 end        
                                              when ma_pb in ('VNP0701100','VNP0701200','VNP0701300','VNP0701400','VNP0701500','VNP0701600','VNP0701800','VNP0702100','VNP0702200')
                                                        and loaitb_id=58 and loaihd_id=1 and goi_id is null                         -- 9PBHKV lap moi Fiber khong tham gia goi tich hop
                                                    then 1.1
                                              when loaitb_id=271                                                                                   -- MyTV OTT: <6t : 0.2 ; tu 6t-> duoi 12t: 0.3 ; tu 12t tro len: 0.4
                                                    then decode(to_number(regexp_replace (goi_cuoc, '\D', '')) , 0,0.2, 1,0.2, 2,0.2, 3,0.2, 4,0.2, 5,0.2, 6,0.3, 7,0.3, 8,0.3, 9,0.3, 10,0.3, 11,0.3, 12,0.4, 18,0.4, 0) 
                                            when loaitb_id=200 then    -- Ecabinet
													   case when sothang_dc<3 or sothang_dc is null then 0.1
															  when sothang_dc>=3 and sothang_dc<6 then 0.2
															  when sothang_dc>=6 and sothang_dc<12 then 0.25
															  when sothang_dc>=12 then 0.35 else null 
													   end
                                            when loaitb_id in (35,121,120) then      -- eGov (VNPT iGate (121), iOffice (35), VNPT Portal (120), ...) : hop dong theo thang tron goi, tra sau (hinh thuc cho thue) => hsdv =1;  hop dong theo thang tron goi, tra truoc => hsdv = 0.4
															   case when nvl(datcoc_csd,0)>0 
																								  and (select nvl(muccuoc_tb,0) from ttkd_bct.ptm_codinh_202405
																												where thuebao_id=a.thuebao_id and hdtb_id=a.hdtb_id
																										)>0                                                
																		    then 0.4  -- thue thang , nhung co dat coc => tinh nhu dthu tron hop dong
																	  when nvl(datcoc_csd,0)=0
																							  and (select nvl(muccuoc_tb,0) from ttkd_bct.ptm_codinh_202405 
																											where thuebao_id=a.thuebao_id and hdtb_id=a.hdtb_id
																									) > 0                                     
																		    then 1        
--																	  when (select nvl(muccuoc_tb,0) from ttkd_bct.ptm_codinh_202405 
--																							where thuebao_id=a.thuebao_id and hdtb_id=a.hdtb_id
--																					) = 0
--																		    then 0.4  -- dthu tron hop dong 
																		else 0.4		---Update 4/6/24 thay the FRAME muctb = 0
																  end
                                                when loaitb_id in (11,58,61,210) and kieuld_id=96 then 0.3      -- tai lap
                                                when loaitb_id=153 and loaihd_id=41 then 
														case when sothang_dc>=6 then 0.3 else 0 end                     -- Gia han VNPT SmartCloud theo VB 167/TTr-NS-DH 23/05/2022: chi ghi nhan khi gia han goi 6 thang tro len
                                                when loaitb_id=296 then                                                                -- VNPT Home-Clinic: theo thang : 1, theo gói 6t,12t: 0.3 , VB 328/TTKD HCM-DH 31/12/2021
															case when sothang_dc>=6 then 0.3 else 1 end
                                                when loaitb_id in (279, 317, 287) then                                         -- VNPT AntiDDoS: theo thang =1, theo thuê dich vu 72 gio = 0.3, eoffice 718660 
                                                                                                                                                                    -- VNPT IOC (Trung tam Dieu hanh thong minh), VNPT eDIG (Phan mem He thong quan ly Ho so)
														  case when exists (select muccuoc_tb, datcoc_csd from ttkd_bct.ptm_codinh_202405
																								 where loaitb_id=279 and nvl(muccuoc_tb,0)=0 and datcoc_csd>0 
																												and thuebao_id=a.thuebao_id and hdtb_id=a.hdtb_id
																							) 
																    then 0.3 else (select heso_dichvu from ttkd_bsc.dm_loaihinh_hsqd b where a.loaitb_id=b.loaitb_id)
														   end
                                                when loaitb_id in (317,287,285,279) then                                            --  VNPT AntiDDoS ,VNPT IOC, VNPT eDIG, VNPT AI Camera
												   case when not exists (select 1 from ttkd_bct.ptm_codinh_202405 
																								where nvl(muccuoc_tb,0)=0 and hdtb_id=a.hdtb_id
																							)
																	 then 1 else (select heso_dichvu from ttkd_bsc.dm_loaihinh_hsqd b where a.loaitb_id=b.loaitb_id) 
													end     -- -- thue phan mem theo thang: 1 ; Mua tron goi phan mem: 0.3
                                     
                                            else (select heso_dichvu from ttkd_bsc.dm_loaihinh_hsqd b where a.loaitb_id=b.loaitb_id)                       
											 end
						  , heso_dichvu_1 = case when loaitb_id = 131 then 0.004 end  
				    -- select thang_luong, thang_ptm, thang_tldg_dt, heso_dichvu_1, heso_dichvu, nguon from ttkd_bsc.ct_bsc_ptm a
				    where 
						(thang_ptm = 202405 --- thang n
								or thang_luong in (2,4))			---flag 4 file so 5 import dung thu chuyen dung that, flag 2 update Dthu_goi = datcoc_csd file 6
								--and thang_tldg_dt is null
								and loaitb_id not in (20, 21,149) and nguon not like '%ct_ptm_ngoaictr_imp%' and id_447 is null --and heso_dichvu is null
				    ; 
				commit;                                                 
 
                
/*-- Luu y SMS Brandname neu ghi nhan theo gia tri hop dong => pbh gui ds thue bao ve PDH duyet
update ct_bsc_ptm a  
    set  heso_dichvu = (case when dthu_goi >=1000000000 then 0.05 else 0.08 end)
           ,heso_dichvu_1=0.004
    where thang_ptm=202404 and loaitb_id=131 and ...;
*/
                  
/*    
select ma_tb, dich_vu, tenkieu_ld, dichvuvt_id, loaitb_id , doituong_id, tocdo_id, ghi_chu,dthu_ps , dthu_goi, loaihd_id, kieuld_id from ct_bsc_ptm 
    where thang_ptm=202404 and (loaitb_id not in (21,210) or ma_kh='GTGT rieng') and heso_dichvu is null;           
*/
  
                                
-- He so khu vuc dac thu: danh muc quan ly thay doi from PDH.
			delete ttkd_bsc.dm_duan_dacthu where thang=202404
			;
			select * from ttkd_bsc.dm_duan_dacthu where thang=202405
			;
			desc ttkd_bsc.dm_duan_dacthu;
			
			insert into ttkd_bsc.dm_duan_dacthu 
					    select 202405 thang, ma_da, ten_duan, heso_kvdacthu, toanha_id, ghichu
					    from ttkd_bsc.dm_duan_dacthu a
					    where not exists (select 1 from ttkd_bsc.dm_duan_dacthu where thang = 202405 and ma_da=a.ma_da)    
							  and thang = 202404
							  ;

    
				update ttkd_bsc.ct_bsc_ptm a 
				    set heso_kvdacthu=null
			---	select heso_kvdacthu from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm = 202405 and dichvuvt_id!=2
					   and ma_da is not null and heso_kvdacthu is not null
					   and exists (select 1 from ttkd_bsc.dm_duan_dacthu where thang = 202405 and ma_da=a.ma_da)
					   ;
            
            
			update ttkd_bsc.ct_bsc_ptm a 
			    set heso_kvdacthu = (select heso_kvdacthu from ttkd_bsc.dm_duan_dacthu where thang = a.thang_ptm and ma_da=a.ma_da)
			---	select thang_luong, nguon, heso_kvdacthu, ma_da from ttkd_bsc.ct_bsc_ptm a
			    where (a.thang_ptm = 202405 --- thang n
								or thang_luong  < 100)			---flag 4 file so 5 import dung thu chuyen dung that 
				   and ma_da is not null --and heso_kvdacthu is null
				   and exists (select 1 from ttkd_bsc.dm_duan_dacthu where thang = a.thang_ptm and ma_da=a.ma_da)
--and thang_luong <100
				   ;
                              commit;

-- Shop / myvnpt / ctvxhh
-- Dot 1 ngay 5: chay het 2 dot---Ngung chay, chuyen qua heso_hotro_nvptm va heso_hotro_nvhotro
			update ttkd_bsc.ct_bsc_ptm a 
			    set  ghi_chu = (select distinct nguoi_cn_goc from ttkd_bct.ptm_codinh_202405
									  where nguoi_cn_goc in ('myvnpt','dhtt.mytv','ws_smes') and hdtb_id=a.hdtb_id) || decode(ghi_chu,null,null,' - ') || ghi_chu
					  , heso_kvdacthu = 0.5
					  , thang_luong = 89
		---	select nguon, heso_kvdacthu, ghi_chu, ma_pb from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202405 --and ma_pb='VNP0703000'      
				   and exists (select 1 from ttkd_bct.ptm_codinh_202405
								   where nguoi_cn_goc in ('myvnpt','dhtt.mytv','ws_smes') and hdtb_id=a.hdtb_id)
			;
                                    
   commit;
   --da xong 1g5phut ngay 16/06
-- Dot 2 : chay het 2 dot   ---Ngung chay, chuyen qua heso_hotro_nvptm va heso_hotro_nvhotro
				update ttkd_bsc.ct_bsc_ptm a 
				    set heso_kvdacthu= (case -- when exists (select 1 from dulieu_ftp.hcm_br_online_202404@vinadata where ma_gd=a.ma_gd and ma_tb=a.ma_tb and mahrm=a.manv_ptm) then 1  
														  when exists(select 1 from ttkd_bsc.digishop a 
																			 where thang>=202402 and lower(trangthai_shop) like 'th_nh c_ng' and ma_gioithieu = a.manv_ptm) 
															 then 1 else 0.5 end)    
				    /*-- select 
								(select nguoi_cn_goc from ttkd_bct.ptm_codinh_202404 
											 where nguoi_cn_goc in ('freedoo','myvnpt','dhtt.mytv','shop.vnpt.vn','ws_smes') and thuebao_id=a.thuebao_id) nguoi_cn_goc,
								case when exists(select 1 from ttkd_bsc.digishop a 
													   where thang=202404 and lower(trangthai_shop) like 'th_nh c_ng' and ma_gioithieu=a.manv_ptm) then 'web Didishop'
									   when exists(select 1 from khanhtdt_ttkd.imp_shopctv_dh_2024 where ma_dhsxkd=a.ma_gd_gt) then 'web shop-ctv' end nguon,
								a.ma_gd, ma_gd_gt, a.ma_tb, ungdung_id, ghi_chu, ma_tiepthi, ten_pb 
						  from ct_bsc_ptm a
						  -- */
				---	select heso_kvdacthu from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202405 and ma_pb='VNP0703000' 
					   and (ungdung_id is not null
										 or exists(select 1 from ttkd_bsc.digishop a 
																where thang=202405 and lower(trangthai_shop) like 'th_nh c_ng' and ma_gioithieu=a.manv_ptm
													  ) 
										  or exists (select 1 from ttkd_bct.ptm_codinh_202405
																where nguoi_cn_goc in ('myvnpt', 'dhtt.mytv', 'ws_smes') and thuebao_id=a.thuebao_id
															)
							)
				;


-- He so tra truoc: 
		update ttkd_bsc.ct_bsc_ptm a 
				set heso_tratruoc=null 
	---	select nguon, heso_tratruoc from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202405 and loaitb_id!=20 and nguon not like 'ct_ptm_ngoaictr%'  --loai tru cac file tu excel A Nghia
			   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id)
			   ;
 
            
			update ttkd_bsc.ct_bsc_ptm a 
			    set heso_tratruoc = (case when sothang_dc>16 then 1.2
															   when sothang_dc>=12 and sothang_dc<=16 then 1.15
															   when sothang_dc>=6 and sothang_dc<12 then 1.1
															   when sothang_dc is not null and sothang_dc<6 then 1
												   else 1 end)
			---	select thang_luong, thang_ptm, ma_tb, thang_tldg_dt, sothang_dc, heso_tratruoc from ttkd_bsc.ct_bsc_ptm a
			    where 
							(thang_ptm = 202405 --- thang n
								or thang_luong in (2,4))			---flag 4 file so 5 import dung thu chuyen dung that, flag 2 update Dthu_goi = datcoc_csd file 6
							and loaitb_id!=20 --and nguon not like 'ct_ptm_ngoaictr%'--not in ('Trong co che tinh luong','Ngoai co che tinh luong')							
							and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id)
							
				   ;
--   hcm_ca_00058242
--hcm_ivan_00023479
commit;

   
-- He so dai ly :  VB 353/TTr-DH-NS - 12/2023:    AM ban hang thong qua Dai ly tinh 50%; nguoc lai AM QLDL ban hang thong qua DAI LY 100%
			update ttkd_bsc.ct_bsc_ptm a 
			    set heso_daily = ''
			    where thang_ptm = 202405 and heso_daily is not null
			    ;
   
    
			update ttkd_bsc.ct_bsc_ptm a 
			    set heso_daily = 0.05
--						, thang_luong = 25
		-- select thang_ptm, manv_ptm, ma_vtcv, ma_nguoigt, heso_daily from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm >= 202402 and manv_ptm is not null and heso_daily is null
				   and (upper(ma_tiepthi_new)like'GTGT_%' or upper(ma_tiepthi_new)like'DAILY_%' or upper(ma_tiepthi_new)like'DL_%'
									or upper(ma_nguoigt)like'GTGT_%' or upper(ma_nguoigt)like'DAILY_%' or upper(ma_nguoigt)like'DL_%'
						 )
				   and ma_vtcv not in ('VNP-HNHCM_KHDN_3.1','VNP-HNHCM_KHDN_2','VNP-HNHCM_KHDN_1') 
				  -- and ma_nguoigt in ('DL_HOANGKIM', 'GTGT00165','GTGT00166', 'GTGT00173')
				   --, 'GTGT00181','GTGT00182', 'GTGT00186','GTGT00164','GTGT00083','GTGT00125', 'GTGT00194')
--				   and ma_nguoigt in (select MA_DAILY  from ttkd_bsc.dm_daily_khdn 
--															where ma_vtcv not in ('VNP-HNHCM_KHDN_3.1','VNP-HNHCM_KHDN_2','VNP-HNHCM_KHDN_1')
--																			and thang = a.thang_ptm
--													)
--					and ma_tb in ('hcm_colo_00010720', 'hcm_colo_00010838', 'hcm_econtract_00000999')
				   
				   ;
				   
   
commit;
 
-- HE SO QUY DINH: ban hang ngoai dia ban --> 50%; nguoc lai 100%
			update ttkd_bsc.ct_bsc_ptm 
					set heso_quydinh_nvptm = null
			    where thang_ptm = 202405 and (loaitb_id<>21 or ma_kh='GTGT rieng') 
			    ;
                                

				update ttkd_bsc.ct_bsc_ptm a
				    set heso_quydinh_nvptm = case when chuquan_id = 264 or ma_kh='GTGT rieng' or nhom_tiepthi=2 then 1
																			  else case when diaban in ('Trong CT ban hang','Khong xet trong/ngoai CT ban hang') then 1
																							when diaban='Ngoai CT ban hang' then 0.5 else 1 end
														   end
						, heso_quydinh_nvhotro = case when chuquan_id = 264 or ma_kh='GTGT rieng' or nhom_tiepthi=2 then 1
																			  else case when diaban in ('Trong CT ban hang','Khong xet trong/ngoai CT ban hang') then 1
																							when diaban='Ngoai CT ban hang' then 0.5 else 1 end
																	end
						, heso_quydinh_dai = case when manv_tt_dai is not null then 1 else null end
				
--				  select nguon, thang_luong, ma_tb, heso_quydinh_nvptm, heso_quydinh_nvhotro, heso_hotro_nvhotro, manv_hotro, heso_quydinh_dai, manv_tt_dai, diaban, chuquan_id, nhom_tiepthi from ttkd_bsc.ct_bsc_ptm a    
				    where 
								(thang_ptm >= 202402 --- thang n -3
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
					and (loaitb_id<>21 or ma_kh='GTGT rieng') --and heso_quydinh_nvhotro is null
				    --and a.ma_duan_banhang = '235405'
--				    and thang_luong in (26)
--					and thang_luong= 99
--					and thang
				    
				    ;
                                    
commit;
  
/* Kiem tra:   
select * from ct_bsc_ptm 
    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') and heso_quydinh_nvptm is null; 

select * from ct_bsc_ptm 
    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng' )
        and manv_tt_dai is not null and heso_quydinh_dai is null; 

select * from ct_bsc_ptm 
    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng' )
        and manv_hotro is not null and heso_quydinh_nvhotro is null;                                     

*/    
 
     
-- he so vtcv: vtcv nao duoc tinh
				update ttkd_bsc.ct_bsc_ptm 
					   set heso_vtcv_nvptm='', heso_vtcv_dai='', heso_vtcv_nvhotro=''
				    where thang_ptm=202405 and (loaitb_id<>21 or ma_kh='GTGT rieng')
				    ;
             
 
				update ttkd_bsc.ct_bsc_ptm a 
					   set   heso_vtcv_nvptm = 1
							 ,heso_vtcv_nvhotro = 1--case when manv_hotro is not null then 1 else null end
							 ,heso_vtcv_dai = 1--case when manv_tt_dai is not null then 1 else null end 
			--	    select heso_vtcv_nvptm, heso_vtcv_nvhotro, heso_vtcv_dai, manv_hotro from ttkd_bsc.ct_bsc_ptm a 
				    where (thang_ptm >= 202402 --- thang n -3
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
					and (loaitb_id<>21 or ma_kh='GTGT rieng') 
--					and thang_luong in (99)
				    
				    
				    ;
             
    commit;

-- He so khach hang: 
    -- cdbr+gtgt
			update ttkd_bsc.ct_bsc_ptm a set phanloai_kh=''
	---select phanloai_kh from ttkd_bsc.ct_bsc_ptm a 
			    where  thang_ptm = 202405 and phanloai_kh is not null-- and thang_luong = 86
			    ; 
         
           
-- Da co file giao:        khong chay dot 1               
			update ttkd_bsc.ct_bsc_ptm a
			    set phanloai_kh = case when ma_kh='GTGT rieng' then
													  (select (select ma_plkh 
																	from css_hcm.phanloai_kh where phanloaikh_id=b.plkh_id) 
														 from ttkd_bct.db_thuebao_ttkd b where to_number(regexp_replace(b.mst, '\D', '')) = to_number(regexp_replace(a.mst, '\D', ''))  and rownum=1
														 )  
												when ma_kh<>'GTGT rieng' and loaitb_id not in (20, 21, 149)  
													  then (select distinct (select ma_plkh from css_hcm.phanloai_kh where phanloaikh_id=b.plkh_id ) 
															    from ttkd_bct.db_thuebao_ttkd b where b.thuebao_id=a.thuebao_id)
												when loaitb_id = 271 then 'DC2'
												when loaitb_id in (20,149) 
													  then (select (select ma_plkh from css_hcm.phanloai_kh where phanloaikh_id=b.plkh_id) 
															    from ttkd_bct.db_thuebao_ttkd b 
															    where loaitb_id=20 and b.ma_tb=a.ma_tb and to_char(ngay_sd,'dd/mm/yyyy')=to_char(a.ngay_bbbg,'dd/mm/yyyy'))
										 end
			-- select phanloai_kh, chuquan_id ,ma_kh, ma_tb, nguon from ttkd_bsc.ct_bsc_ptm a 

			    where (thang_ptm = 202405 --- thang n
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
					and loaitb_id<>21 and phanloai_kh is null  --and a.ma_tb = '84853136618'
					
			    ;
                             commit;   
                                    rollback;
                                    
-- Chua co file giao (don gia ngay 5):     chay dot 1
			drop table hocnq_ttkd.temp_plkh purge
			;
			create table hocnq_ttkd.temp_plkh as
			    select distinct a.*, (select min(ma_plkh) from ttkd_bct.dbkh_plkh pl, css_hcm.phanloai_kh plkh 
														where pl.plkh_id=plkh.phanloaikh_id and ma_dt_kh='dn' and pl.khachhang_id=a.khachhang_id_plkh) phanloai_kh
				   from (select khachhang_id, (select max(khachhang_id) 
																		from ttkd_bct.db_thuebao_ttkd where trangthaitb_id not in (7,8,9) and ma_dt_kh='dn' and khachhang_id_goc=a.khachhang_id) khachhang_id_plkh
							 from ttkd_bsc.ct_bsc_ptm a
							 where thang_ptm = 202405 and doituong_kh='KHDN' ) a
							 ;
             
				update ttkd_bsc.ct_bsc_ptm a 
				    set phanloai_kh = (case when exists(select 1 from ttkd_bct.dbkh_db where nhom in ('COOP','MAILINH') and regexp_replace(mst, '\D', '') = regexp_replace(a.mst, '\D', '') ) then 'DA2'  -- dbkh_db bang chi Nguyen
															 when loaitb_id=271 then 'DC2'
															 when ma_kh='GTGT rieng' 
																  then (select (select ma_plkh from css_hcm.phanloai_kh where phanloaikh_id=b.plkh_id) 
																		    from ttkd_bct.db_thuebao_ttkd b where to_number(regexp_replace(b.mst, '\D', '')) = to_number(regexp_replace(a.mst, '\D', ''))  and rownum=1)   
															else (select phanloai_kh from hocnq_ttkd.temp_plkh where khachhang_id=a.khachhang_id)
												end)
				    -- select loaitb_id, dich_vu, ma_tb, ngay_bbbg, nguon, thuebao_id, ma_duan_banhang, chuquan_id, phanloai_kh, doituong_kh from ttkd_bsc.ct_bsc_ptm a
				    where (thang_ptm = 202405 
									or thang_luong = 4)
								and chuquan_id in (145,264,266) and loaitb_id<>21 and phanloai_kh is null
				    ;
 
                    
				update ttkd_bsc.ct_bsc_ptm set heso_khachhang=''
				    where thang_ptm = 202405 and (loaitb_id<>21 or ma_kh='GTGT rieng') and heso_khachhang is not null
				    ;
                   
				update ttkd_bsc.ct_bsc_ptm a
				    set heso_khachhang = case when heso_kvdacthu<1 then 1                                                       -- ap dung cho thue bao thuoc kv doc quyen, Mai linh, Coop, vnpts Hoa Binh 
																   when khhh_khm = 'KHM' then 1		---xem dk nay, de ap dung tu T5/24
																   else case when phanloai_kh in ('DA1','DA2','DB1') then 0.7
																					 when phanloai_kh in ('DB2') then 0.85
																					 when phanloai_kh in ('DB3','DC1','DC2') then 1
																					 else 1 end end
--								, thang_luong = 24
				-- select thang_luong, heso_khachhang, heso_kvdacthu, doituong_kh, khhh_khm, phanloai_kh, nguon, thuebao_id, ma_duan_banhang, khachhang_id, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a
				    where (thang_ptm = 202405 
									or thang_luong = 4) and (loaitb_id<>21 or ma_kh='GTGT rieng') --and heso_khachhang is null
							--	and ma_gd in ('HCM-LD/14628609')
				    ;
--				select * from ttkd_bct.db_thuebao_ttkd where ma_tb = '84845911095';
--				select * from css_hcm.phanloai_kh;
          commit;
		rollback;
/*
select heso_khachhang , count(*)
    from ct_bsc_ptm
    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') 
    group by heso_khachhang;   
*/
 
        
-- He so thue bao ngan han: 
			update ttkd_bsc.ct_bsc_ptm a 
					set heso_tbnganhan=0.3
			-- 	select heso_tbnganhan from ttkd_bsc.ct_bsc_ptm a
			    where (a.thang_ptm = 202405 --- thang n
								or a.thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
					and thoihan_id=1 and dichvuvt_id in (1,10,11,4,7,8,9)
			    ;
                           commit;
                       
-- He so dich vu DNHM+sodep:
				update ttkd_bsc.ct_bsc_ptm a 
						set heso_dichvu_dnhm=null
--			select tien_dnhm, tien_sodep, heso_dichvu_dnhm from ttkd_bsc.ct_bsc_ptm a
				    where (thang_ptm = 202405
										or thang_luong = 4)
								and loaitb_id<>21 and ma_kh<>'GTGT rieng'
					   and (tien_dnhm>0 or tien_sodep>0)
					   ;   
        
                                    
				update ttkd_bsc.ct_bsc_ptm a 
					   set heso_dichvu_dnhm = (case when loaitb_id in (280, 292) then 0.3  -- phi tich hop
																   when loaitb_id=35 then 0.4       -- VNPT iOffice-phi tich hop nhap o tien dnhm
																   else 0.1 end)
				    -- select heso_dichvu_dnhm, loaitb_id from ttkd_bsc.ct_bsc_ptm a
				    where (thang_ptm = 202405 --- thang n
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
								and (loaitb_id<>21 or ma_kh='GTGT rieng')
				    ;
                          commit;          
        
-- he so ho tro:
				---Bsung Thong tin QLDA
				UPDATE
							set ma_duan_banhang =
					where
				;
				
				MERGE into ttkd_bsc.ct_bsc_ptm a
							using (with 
													yc_dv as (select ma_yeucau, id_ycdv, ma_dichvu, row_number() over(partition by MA_YEUCAU, MA_DICHVU order by NGAYCAPNHAT desc) rnk
																	from ttkdhcm_ktnv.amas_yeucau_dichvu
																	)
													, t as	 (select c.manv_presale_hrm, c.tyle/100 tyle_hotro, decode(tyle_am,0,1,c.tyle_am/100) tyle_am, d.loaitb_id_obss, b.ma_yeucau, b.ma_dichvu, c.tyle_nhom
																		, NGAYHEN, NGAYCAPNHAT, NGAYNHANTIN_PS, NGAYXACNHAN
																 from yc_dv b, ttkdhcm_ktnv.amas_booking_presale c, ttkdhcm_ktnv.amas_loaihinh_tb d
																					where b.ma_yeucau=c.ma_yeucau and b.id_ycdv=c.id_ycdv and b.ma_dichvu = d.loaitb_id
																								and c.tyle>0 and c.ps_truong=1 and c.xacnhan=1  
																)
												select MANV_PRESALE_HRM, LOAITB_ID_OBSS, MA_YEUCAU, MA_DICHVU, TYLE_AM, sum(tyle_hotro) tyle_hotro, sum(TYLE_NHOM) TYLE_NHOM
															from t
														--	where ma_yeucau not in ()
															group by MANV_PRESALE_HRM, LOAITB_ID_OBSS, MA_YEUCAU, MA_DICHVU, TYLE_AM
															
										) b
							ON (b.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', ''))
												and b.loaitb_id_obss = a.loaitb_id)
							WHEN MATCHED THEN
									UPDATE SET a.manv_hotro = b.manv_presale_hrm
															, a.tyle_hotro = b.tyle_hotro
															, a.tyle_am = b.tyle_am
															, thang_luong = 26
															
					---  select manv_hotro, tyle_hotro, tyle_am, ma_duan_banhang, loaitb_id, ma_gd from ttkd_bsc.ct_bsc_ptm a
							where ma_duan_banhang is not null --and manv_hotro is  null
										and thang_tldg_dt_nvhotro is null
										and thang_ptm >= 202402		---thang n -3
										 and exists (select distinct c.manv_presale_hrm, c.tyle/100, b.ma_yeucau, d.loaitb_id_obss
															from ttkdhcm_ktnv.amas_yeucau_dichvu b, ttkdhcm_ktnv.amas_booking_presale c, ttkdhcm_ktnv.amas_loaihinh_tb d
															where b.ma_yeucau = c.ma_yeucau and b.id_ycdv = c.id_ycdv and b.ma_dichvu = d.loaitb_id
																	  and c.tyle>0 and ps_truong = 1 and xacnhan = 1    
																	  and b.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', ''))
																	  and d.loaitb_id_obss = a.loaitb_id 
															)
--									and ma_duan_banhang in ('199069', '243885')
--										and thang_luong = 26
								;

				update ttkd_bsc.ct_bsc_ptm set heso_hotro_nvptm='' 
--				    select heso_hotro_nvptm from ttkd_bsc.ct_bsc_ptm
				    where thang_ptm = 202405 and (loaitb_id<>21 or ma_kh='GTGT rieng') and heso_hotro_nvptm is not null
				    ;
												    
				update ttkd_bsc.ct_bsc_ptm a
				    set heso_hotro_nvptm  = case 
																	when a.loaitb_id = 20 and exists (select sdt_datmua, ma_gioithieu, tennv_gioithieu 
																																from ttkd_bsc.digishop 
																																where lower(trangthai_shop) like 'th_nh c_ng' and sdt_datmua = a.ma_tb)
																				then 0.5				---50% neu VNPts ban qua DIGISHOP
																	when a.dichvuvt_id not in (2, 14, 15, 16) 
																											and exists (select ma_dhsx, ma_gioithieu, tennv_gioithieu 
																																from ttkd_bsc.digishop 
																																where lower(trangthai_shop) like 'th_nh c_ng' and ma_dhsx = a.ma_gd_gt)
																				then 0.5			---50% neu dvu BR, CD ban qua DIGISHOP
--																	when manv_hotro is not null and tyle_am is null then 0.5			ap dung toi uu, dang nghien cuu--50% cho cong doan nguoi xu ly, case hok khong PS
																	   when exists (select 1 from ttkd_bct.ptm_codinh_202405
																							   where nguoi_cn_goc in ('myvnpt','dhtt.mytv','ws_smes') and hdtb_id=a.hdtb_id)
																				then 0.5				---he thong khac tinh 50%
																	   when manv_tt_dai is not null and manv_tt_dai<>manv_ptm then 0.5
																	   when manv_hotro is not null and tyle_am is not null then tyle_am 		---AM 1 phan, PS ho tro du an
																	   when manv_ptm='VNP017772' and loaitb_id=288 
																				 and (hdtb_id, thuebao_id) in (select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL where thang = a.thang_ptm)
																							then 0.5   -- SmartCA tinh cho Huong BHOL 50%
																	  else 1 end
						,heso_hotro_nvhotro = case 
																		when a.loaitb_id = 20 and exists (select sdt_datmua, ma_gioithieu, tennv_gioithieu 
																																from ttkd_bsc.digishop 
																																where lower(trangthai_shop) like 'th_nh c_ng' and sdt_datmua = a.ma_tb)
																					then 0.5				---50% neu VNPts ban qua DIGISHOP
																		when a.dichvuvt_id not in (2, 14, 15, 16) 
																												and exists (select ma_dhsx, ma_gioithieu, tennv_gioithieu 
																																	from ttkd_bsc.digishop 
																																	where lower(trangthai_shop) like 'th_nh c_ng' and ma_dhsx = a.ma_gd_gt)
																					then 0.5			---50% neu dvu BR, CD ban qua DIGISHOP
--																		when manv_hotro is not null then 0.5  		--50% cho cong doan nguoi gioi thieu, case hok khong PS
																		when exists (select 1 from ttkd_bct.ptm_codinh_202405
																							   where nguoi_cn_goc in ('myvnpt','dhtt.mytv','ws_smes') and hdtb_id=a.hdtb_id)
																				then 0.5				---he thong khac tinh 50%
																		when manv_hotro is not null and tyle_hotro is not null then tyle_hotro		---PS ho tro du an
																		when manv_ptm='VNP017772' and loaitb_id=288 
																				 and (hdtb_id, thuebao_id) in (select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL where thang = a.thang_ptm)
																							then 0.5   -- SmartCA tinh cho Huong BHOL 50%
																		else 0 end
						,heso_hotro_dai         = case when manv_tt_dai is not null and manv_tt_dai<>manv_ptm then 0.5 else 0 end
						--, thang_luong = 11
						
--		select ma_tb, dich_vu, manv_hotro, tyle_hotro, tyle_am,  heso_hotro_nvptm, heso_hotro_nvhotro, heso_hotro_dai, ma_duan_banhang from ttkd_bsc.ct_bsc_ptm a    
				where (thang_ptm >= 202402 --- thang n -3
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
					 and (loaitb_id<>21 or ma_kh='GTGT rieng') 
--					 and thang_luong = 99
							-- and (hdtb_id, thuebao_id) in ( select hdtb_id, thuebao_id from hocnq_ttkd.ptm_xuly_khieunai_BHOL_202404 )
							--and (hdtb_id, thuebao_id) in ( select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL )
--					and ma_tb = 'DI001042512'		
--					and MANV_HOTRO is not null and TYLE_HOTRO is not null and HESO_HOTRO_NVHOTRO is null
				    ; 
                         commit;  
					rollback;

/* 
select dich_vu, ma_tb, loaitb_id, manv_hotro, tyle_hotro, heso_hotro_nvptm, heso_hotro_nvhotro from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm=202405 
				and a.loaitb_id = 20 and exists (select sdt_datmua, ma_gioithieu, tennv_gioithieu 
																																from ttkd_bsc.digishop 
																																where lower(trangthai_shop) like 'th_nh c_ng' and sdt_datmua = a.ma_tb)
										;
    
select dich_vu, ma_tb, loaitb_id, manv_hotro, tyle_hotro, heso_hotro_nvptm, heso_hotro_nvhotro from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm=202405 
				and a.dichvuvt_id not in (2, 14, 15, 16) 
				and exists (select ma_dhsx, ma_gioithieu, tennv_gioithieu 
									from ttkd_bsc.digishop 
									where lower(trangthai_shop) like 'th_nh c_ng' and ma_dhsx = a.ma_gd_gt)
					;

select dich_vu, ma_tb, loaitb_id, tennv_ptm, manv_hotro, tyle_hotro, heso_hotro_nvptm, heso_hotro_nvhotro from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm=202405 
				and manv_ptm='VNP017772' and loaitb_id=288 
				and (hdtb_id, thuebao_id) in (select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL where thang = a.thang_ptm)
					;
*/


-- He so dia ban tinh khac: 
			update ttkd_bsc.ct_bsc_ptm 
				   set heso_diaban_tinhkhac = 0.85       
	---		select heso_diaban_tinhkhac from ttkd_bsc.ct_bsc_ptm a
			    where (thang_ptm = 202405 --- thang n
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
					and loaitb_id not in (20,21,149) and heso_diaban_tinhkhac is null
				   and ( (dichvuvt_id in (1,10,11,12,4,7,8,9) and kieuld_id=13102)
									or (dichvuvt_id in (13,14,15,16) and tinh_id not in (28, 8,36))
						 ) 
				;
           
-- Luu y kiem tra heso_diaban_tinhkhac cho GTGT rieng

        
/*-- Kiem tra:
select dich_vu, ma_tb, heso_diaban_tinhkhac, diachi_ld, tinh_id, ttvt_id from ttkd_bsc.ct_bsc_ptm
    where thang_ptm=202405 and (loaitb_id not in (20,21,149) or ma_kh='GTGT rieng' ) 
        and heso_diaban_tinhkhac is not null;             
*/
 

-- Kiem tra thue bao tinh doanh thu nhieu lan trong thang:
select ma_gd, ma_tb, nguon, loaihd_id, dthu_goi, kieuld_id,lydo_khongtinh_luong, thang_tldg_dt, lydo_khongtinh_dongia
    from ttkd_bsc.ct_bsc_ptm
    where thang_ptm = 202405 and loaitb_id<>21 and
                (ma_gd, ma_tb, loaitb_id) in 
                          (select ma_gd, ma_tb, loaitb_id from ttkd_bsc.ct_bsc_ptm 
                            where thang_ptm = 202405 and loaitb_id!=20 group by ma_gd, ma_tb, loaitb_id having count(*)>1
					   )
				;
                            
-- Vua tai lap dich vu (loaihd_id=7, ngung su dung qua 35 ngay) vua nang/ha cap : xet doanh thu tai lap dich vu theo goi moi (loaihd_id=7)
update ttkd_bsc.ct_bsc_ptm a 
		set lydo_khongtinh_luong=lydo_khongtinh_luong||'-Tinh luong theo hs tai lap'
--    select * from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm = 202405 and nguon like 'thaydoitocdo%' and lydo_khongtinh_luong not like '%Tinh luong theo hs tai lap%'
        and exists (select 1 from ttkd_bsc.ct_bsc_ptm
                            where thang_ptm = 202405 and thuebao_id=a.thuebao_id and nguon like 'tailap%')
					   ;
		
		commit;


-- DTHU DON GIA 
			update ttkd_bsc.ct_bsc_ptm a 
						set dongia = 858
		---	    select nguon, dongia, dichvuvt_id, thang_ptm from ttkd_bsc.ct_bsc_ptm a
			    where (thang_ptm >= 202402 --- thang n
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
					and (loaitb_id<>21 or ma_kh='GTGT rieng' ) and dongia is null 
			    ;                            
           commit;
           
-- cac dv ngoai tru VNPTT, SMS Brandname, Voice Brandnanme
			update ttkd_bsc.ct_bsc_ptm a 
			    set doanhthu_dongia_nvptm   = round(dthu_goi*nvl(tyle_huongdt,1) *heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																		* heso_quydinh_nvptm * heso_vtcv_nvptm * nvl(heso_kvdacthu,1)
																		* heso_hotro_nvptm * nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
																		* nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)* nvl(heso_daily,1) ,0)                                                                            
					,doanhthu_dongia_nvhotro = round(dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																		* heso_quydinh_nvhotro * heso_vtcv_nvhotro * nvl(heso_kvdacthu,1)
																		* heso_hotro_nvhotro * nvl(heso_khachhang,1) * nvl(heso_tbnganhan,1)
																		* nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) ,0)
					,doanhthu_dongia_dai          = round(dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																		*heso_quydinh_dai * heso_vtcv_dai * nvl(heso_kvdacthu,1)
																		* heso_hotro_dai * nvl(heso_khachhang,1) * nvl(heso_tbnganhan,1)
																		*nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) ,0)
	---	    select nguon, sothang_dc, thang_ptm, ma_tb, dichvuvt_id, dthu_goi, doanhthu_dongia_nvptm, doanhthu_dongia_nvhotro, doanhthu_dongia_dai, ma_kh, loaitb_id from ttkd_bsc.ct_bsc_ptm a
			    where (thang_ptm = 202405 --- thang n
								or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
							and (loaitb_id not in (21,131,358) or ma_kh='GTGT rieng')  and dthu_goi >0
							and (nvl(thang_tldg_dt, 999999)>=202405 and nvl(thang_tlkpi_phong, 999999)>=202405)
			   -- and thang_luong = 87
			  -- and ma_tb = 'hcm_ivan_00026696'
			    ;                       

            commit;
-- Doanh thu goi tich hop: ap dung khong phan biet tap quan ly 
        -- tham khao bang huong dan cua anh Nghia tinh he so cac goi tich hop cho SMEs: VB 275/TTKD HCM-DH 22/06/2020:
				update ttkd_bsc.ct_bsc_ptm a
				    set doanhthu_dongia_nvptm =
								    (case when goi_id=15599  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.21/0.1434 ,0)  -- SME_NEW
											 when goi_id=15600  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.25/0.17 ,0)  -- SME+
											 when goi_id=15602  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.25/0.21 ,0)  -- SME_BASIC 1
											 when goi_id=15601  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.35/0.30 ,0)  -- SME_BASIC 2   
											 when goi_id=15604  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.19/0.13 ,0)  -- SME_SMART1
											 when goi_id=15603  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.20/0.14 ,0)  -- SME_SMART2
											 when goi_id=15605  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.20/0.16 ,0)  -- F_Pharmacy
											 when goi_id=15596  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.20/0.15 ,0)  -- F_ORM
									end)   
	---	    select doanhthu_dongia_nvptm from ttkd_bsc.ct_bsc_ptm a
				    where (thang_ptm = 202405 --- thang n
								or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
					and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605) 
				    ;
                      
                            
-- Luong don gia cac dv ngoai tru vnptt, SMS Brandname:
				update ttkd_bsc.ct_bsc_ptm a 
				    set luong_dongia_nvptm    = round(nvl(doanhthu_dongia_nvptm,0)*dongia/1000 ,0)
						,luong_dongia_nvhotro = round(nvl(doanhthu_dongia_nvhotro,0)*dongia/1000 ,0)
						,luong_dongia_dai         = round(nvl(doanhthu_dongia_dai,0)*dongia/1000 ,0)
		--- select luong_dongia_nvptm, luong_dongia_nvhotro, luong_dongia_dai from ttkd_bsc.ct_bsc_ptm a 
				    where (thang_ptm = 202405 --- thang n
								or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
							and (loaitb_id not in (21,131,358) or ma_kh='GTGT rieng') 
				    ;
                            
       commit;
-- SMS Brandname thang n-1: 
			update ttkd_bsc.ct_bsc_ptm a 
			    set doanhthu_dongia_nvptm = round(	
																					((nvl(dthu_goi,0)*heso_dichvu)
																							+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1)
																				) *nvl(tyle_huongdt,1)
																				*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm
																				*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)
																				*heso_khachhang*nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)*nvl(heso_daily,1) ,0)
					,doanhthu_dongia_nvhotro = round(
																					((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) 
																					) *nvl(tyle_huongdt,1)
																				*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																				*nvl(heso_quydinh_nvhotro,1)*nvl(heso_vtcv_nvhotro,1)*heso_hotro_nvhotro*nvl(heso_khachhang,1) ,0)
					,doanhthu_dongia_dai =null
---	   select * from ttkd_bsc.ct_bsc_ptm a
			    where (thang_ptm = 202404 --- thang n -1
								or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
					and loaitb_id=131 
			    ;
									    
			update ttkd_bsc.ct_bsc_ptm 
			    set luong_dongia_nvptm = nvl(doanhthu_dongia_nvptm,0)*dongia/1000
			    where (thang_ptm = 202404 --- thang n-1
								or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
					and loaitb_id=131
			    ;
                           
commit;
            
-- don gia dnhm: 

			update ttkd_bsc.ct_bsc_ptm a 
			    set doanhthu_dongia_dnhm = round((nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *nvl(tyle_huongdt,1) *heso_dichvu_dnhm
																	 * heso_quydinh_nvptm * nvl(heso_kvdacthu,1) * heso_vtcv_nvptm
																	 * nvl(heso_diaban_tinhkhac,1) * nvl(heso_daily,1) ,0)
		--  select thang_luong, ma_tb, luong_dongia_nvptm, doanhthu_dongia_dnhm, nguon from ttkd_bsc.ct_bsc_ptm a
			    where (thang_ptm = 202405 --- thang n
								or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
					
							   and (loaitb_id not in (20,21) or ma_kh='GTGT rieng' or (loaitb_id=20 and goi_luongtinh is null)) 
							   and (tien_dnhm>0 or tien_sodep>0) 
			--				   and (
							   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd 
												  where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id) 
						 --or loaitb_id=292)
						 ;  

                                                 
				update ttkd_bsc.ct_bsc_ptm a 
							set luong_dongia_dnhm_nvptm = round(nvl(doanhthu_dongia_dnhm,0) * dongia / 1000 ,0)
				    where luong_dongia_dnhm_nvptm is null and doanhthu_dongia_dnhm is  not null           
							   and (thang_ptm = 202405 --- thang n
												or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
								and (loaitb_id not in (20,21) or ma_kh='GTGT rieng' or (loaitb_id=20 and goi_luongtinh is null)) 
							   and doanhthu_dongia_dnhm>0
				;

 commit;

-- DTHU KPI  
-- nhan vien:
				update ttkd_bsc.ct_bsc_ptm a
				    set  doanhthu_kpi_nvptm = doanhthu_dongia_nvptm,
						 doanhthu_kpi_nvdai   = doanhthu_dongia_dai,
						 doanhthu_kpi_nvhotro = doanhthu_dongia_nvhotro,
						 doanhthu_kpi_dnhm    = doanhthu_dongia_dnhm
		--- select doanhthu_kpi_nvptm, doanhthu_kpi_nvdai, doanhthu_kpi_nvhotro, doanhthu_kpi_dnhm, doanhthu_dongia_nvhotro from ttkd_bsc.ct_bsc_ptm a
				    where (thang_ptm = 202405 --- thang n
								or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
									and dich_vu not like 'Thi_t b_ gi_i ph_p%' and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') 
				    ;


-- dtkpi cua dich vu Thiet bi giai phap:  la dthu hop dong x cac he so tinh bsc (ko tinh tren chenh lech thu chi).
					update ttkd_bsc.ct_bsc_ptm a
						   set doanhthu_kpi_nvptm  = round(dthu_goi_goc *nvl(tyle_huongdt,1) * nvl(heso_dichvu,1) * nvl(heso_quydinh_nvptm,1)
																 * decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) * nvl(heso_tbnganhan,1)
																 * nvl(heso_diaban_tinhkhac,1) * nvl(heso_daily,1) ,0)  
							   ,doanhthu_kpi_nvhotro = case when manv_hotro is not null 
																		   then round(dthu_goi_goc *nvl(tyle_huongdt,1) * nvl(heso_dichvu,1) * nvl(heso_quydinh_nvhotro,1) * tyle_hotro 
																				 * nvl(heso_tbnganhan,1) * nvl(heso_diaban_tinhkhac,1) ,0) else null end         
			---	select thang_luong, ma_duan_banhang, nguon, dthu_goi, doanhthu_kpi_nvptm, doanhthu_kpi_nvhotro from 	ttkd_bsc.ct_bsc_ptm a
					    where (thang_ptm = 202405 --- thang n
								or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
									 and dich_vu like 'Thi_t b_ gi_i ph_p%'  
					    ; 
           
                            
            
-- SMS Brandname thang n-1:
			update ttkd_bsc.ct_bsc_ptm
			    set  doanhthu_kpi_nvptm = doanhthu_dongia_nvptm,
					  doanhthu_kpi_nvhotro = doanhthu_dongia_nvhotro
--			    select * from ttkd_bsc.ct_bsc_ptm
			    where (thang_ptm = 202404 --- thang n
								or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
							 and loaitb_id=131
			    ;
                            
            
-- DTHU KPI PHONG 
				update ttkd_bsc.ct_bsc_ptm a
					   set doanhthu_kpi_phong = (case when dich_vu like 'Thi_t b_ gi_i ph_p%' 
																		  then round(dthu_goi_goc *nvl(tyle_huongdt,1) * nvl(heso_dichvu,1) * nvl(heso_khuyenkhich,1) * nvl(heso_tratruoc,1) * nvl(heso_kvdacthu,1)         
																						    * decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) * nvl(heso_khachhang,1) 
																						    * nvl(heso_tbnganhan,1) *nvl(heso_diaban_tinhkhac,1) ,0)
																	  else round(dthu_goi * nvl(tyle_huongdt,1) * nvl(heso_dichvu,1) *nvl(heso_khuyenkhich,1) * nvl(heso_tratruoc,1) * nvl(heso_kvdacthu,1)         
																						    * decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) * nvl(heso_khachhang,1) 
																						    * nvl(heso_tbnganhan,1) *nvl(heso_diaban_tinhkhac,1) ,0) end)
		---   select doanhthu_kpi_phong from ttkd_bsc.ct_bsc_ptm a
				    where (thang_ptm = 202405 --- thang n
								or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
								and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') 
				    ;
                      commit;
    
    
-- kpi goi SME: ko xet heso_quydinh_nvptm vi ko phan biet tap KH
				update ttkd_bsc.ct_bsc_ptm a
				    set doanhthu_kpi_phong=round(dthu_goi *nvl(tyle_huongdt,1) *heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
														  *nvl(heso_kvdacthu,1) *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
														  *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) ,0)
						  ,doanhthu_dongia_nvhotro =null 
						  ,doanhthu_dongia_dai =round(dthu_goi *nvl(tyle_huongdt,1) *heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																					*heso_quydinh_dai*heso_vtcv_dai*heso_hotro_dai*nvl(heso_khachhang,1)
																					*nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1),0)
--				    select * from ttkd_bsc.ct_bsc_ptm
				    where (thang_ptm = 202405 --- thang n
										or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
								and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605)
				    ;

 --  SMS Brandname thang n-1
				update ttkd_bsc.ct_bsc_ptm
				    set doanhthu_kpi_phong= round(((nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  
																		)
														    *nvl(tyle_huongdt,1)
														    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
														    *decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) 
														    *nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
														    *nvl(heso_diaban_tinhkhac,1) *nvl(tyle_huongdt,1) ,0)
				    -- select dthu_goi, dthu_goi_ngoaimang, doanhthu_kpi_nvptm from ttkd_bsc.ct_bsc_ptm
				    where (thang_ptm = 202404 --- thang n-1
											or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
								and loaitb_id=131
				    ;
                            
            commit;
-- Kpi dnhm phong:
		update ttkd_bsc.ct_bsc_ptm a
		    set doanhthu_kpi_dnhm_phong = round( (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *nvl(tyle_huongdt,1) *nvl(heso_dichvu_dnhm,1)
														    * nvl(heso_diaban_tinhkhac,1) ,0)
	---	select doanhthu_kpi_dnhm_phong from ttkd_bsc.ct_bsc_ptm a
		    where (thang_ptm = 202405 --- thang n
								or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
					and (loaitb_id not in (20,21) or ma_kh='GTGT rieng' or (loaitb_id=20 and goi_luongtinh is null)) 
					   and (tien_dnhm>0 or tien_sodep>0)
					   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
					   ;
                                 


--- DTHU KPI TO:
				update ttkd_bsc.ct_bsc_ptm a
				    set doanhthu_kpi_to =  doanhthu_kpi_phong
	---	select doanhthu_kpi_to from ttkd_bsc.ct_bsc_ptm a
				    where (thang_ptm = 202405 --- thang n
									or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
								and (loaitb_id!=21 or ma_kh='GTGT rieng')  
				    ;
                    
               
-- AM ban qua dai ly ma khong phai AM quan ly:
				update ttkd_bsc.ct_bsc_ptm a
				    set doanhthu_kpi_nvptm = (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * dthu_goi *nvl(tyle_huongdt,1) * nvl(heso_tratruoc,1)
															 else heso_daily * dthu_goi *nvl(tyle_huongdt,1) end)
					    ,doanhthu_dongia_nvptm = (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * dthu_goi * nvl(tyle_huongdt,1) * nvl(heso_tratruoc,1)
															 else heso_daily * dthu_goi *nvl(tyle_huongdt,1) end)
					    ,luong_dongia_nvptm = (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * dthu_goi *nvl(tyle_huongdt,1) * nvl(heso_tratruoc,1) * 0.858
															 else heso_daily * dthu_goi *nvl(tyle_huongdt,1) * 0.858 end)
						, doanhthu_kpi_dnhm = (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *nvl(tyle_huongdt,1)
															 else heso_daily * (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *nvl(tyle_huongdt,1) end)
					    ,doanhthu_dongia_dnhm = (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *nvl(tyle_huongdt,1)
															 else heso_daily * (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *nvl(tyle_huongdt,1) end)
					    ,luong_dongia_dnhm_nvptm = (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *nvl(tyle_huongdt,1) * 0.858
															 else heso_daily * (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *nvl(tyle_huongdt,1) * 0.858 end)         
		/*		     select ma_tb, manv_ptm, ma_vtcv, ma_nguoigt, heso_tratruoc, dthu_goi, heso_daily
									, doanhthu_dongia_nvptm,luong_dongia_nvptm, doanhthu_kpi_nvptm 
									,doanhthu_dongia_dnhm,luong_dongia_dnhm_nvptm, doanhthu_kpi_dnhm
							    , (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * dthu_goi * nvl(heso_tratruoc,1) * 0.858
															 else heso_daily * dthu_goi * 0.858 end)  newa
									from ttkd_bsc.ct_bsc_ptm a
		*/			
				    where  (thang_ptm = 202405 --- thang n
												or thang_luong in (1, 2, 4))			---flag 4 file so 5 import dung thu chuyen dung that
									and (loaitb_id!=21 or ma_kh='GTGT rieng') 
							 and heso_daily = 0.05 
				;
                
		commit;
-- ======

-- Kiem tra:
-- Cac thue bao chua co doanhthu_dongia_nvptm:
select thang_ptm, chuquan_id, lydo_khongtinh_luong,nguon,dich_vu, dichvuvt_id,loaitb_id, hdtb_id, thuebao_id,doituong_id,
            ma_gd, ma_tb , ngay_bbbg,thoihan_id,
            manv_ptm, tennv_ptm, ten_pb, 
            goi_id, datcoc_csd, dthu_goi_goc,dthu_goi,dthu_ps,dthu_ps_n1,heso_dichvu,heso_khuyenkhich,heso_tratruoc,heso_quydinh_nvptm,heso_vtcv_nvptm,heso_hotro_nvptm  ,
            doanhthu_dongia_nvptm, luong_dongia_nvptm, trangthai_tt_id
from ttkd_bsc.ct_bsc_ptm a
where thang_ptm = 202405
            and loaitb_id not in (21,210) and ma_kh<>'GTGT rieng' 
            and lydo_khongtinh_luong is null and manv_ptm is not null and dthu_ps is not null
            and doanhthu_dongia_nvptm is null
            and loaitb_id not in (20,61,131,222,224,358) ; -- mytv home combo, sms brn


-- kiem tra doanhthu_dongia_nvptm<>doanhthu_kpi_nvptm:
select thang_ptm, ma_tb, manv_ptm,ma_vtcv, dthu_goi, heso_dichvu, heso_quydinh_nvptm, heso_vtcv_nvptm, 
        heso_khuyenkhich, heso_tratruoc, doanhthu_kpi_nvptm dthu_kpi_nvptm, doanhthu_dongia_nvptm dthu_dongia_nvptm, 
        dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1) dthu_dongia_new 
from ttkd_bsc.ct_bsc_ptm
where thang_ptm = 202405 
            and loaitb_id not in (21,210) and ma_kh<>'GTGT rieng' 
            and lydo_khongtinh_luong is null
            and doanhthu_dongia_nvptm<>doanhthu_kpi_nvptm;


-- nv ho tro chua duoc tinh doanhthu_kpi_nvhotro:
select thang_luong, thang_ptm,dich_vu, dichvuvt_id, loaitb_id, tien_dnhm, tocdo_id, goi_id, dthu_ps, thuebao_id,ma_tb,dthu_goi,doanhthu_kpi_nvhotro , chuquan_id, heso_quydinh_nvhotro
    from ttkd_bsc.ct_bsc_ptm
    where thang_ptm=202405 and (loaitb_id<>21 or ma_kh='GTGT rieng') 
    and manv_hotro is not null and dthu_ps is not null and doanhthu_kpi_nvhotro is null
    ;

-- dnhm:
select dichvuvt_id, loaitb_id, ma_tb, tien_dnhm, tien_sodep, (nvl(tien_dnhm,0)+nvl(tien_sodep,0)),heso_dichvu_dnhm, heso_quydinh_nvptm, nvl(heso_kvdacthu,1), heso_vtcv_nvptm, 
            doanhthu_dongia_dnhm,  luong_dongia_dnhm_nvptm, doanhthu_kpi_dnhm
from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm=202405 and (loaitb_id!=21 or ma_kh<>'GTGT rieng')
                                  and (tien_dnhm>0 or tien_sodep>0)
                                  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id );
             
           
-- SMS Brn:
select ma_tb, dthu_ps, dthu_ps_n1, dthu_goi, dthu_goi_ngoaimang, heso_dichvu, heso_dichvu_1,
            doanhthu_dongia_nvptm, doanhthu_kpi_nvptm, luong_dongia_nvptm, doanhthu_kpi_nvhotro, thang_tlkpi_hotro
    from ttkd_bsc.ct_bsc_ptm
    where thang_ptm = 202404 and loaitb_id=131;

-- TB ngan han:
select thang_ptm, ma_pb,manv_ptm,(select ten_nv from ttkd_bsc.nhanvien_202404 where manv_hrm=a.manv_ptm) ten_nv,
            ma_tb, ten_tb,dich_vu, ngay_bbbg,ngay_cat, trangthaitb_id,songay_sd, heso_tbnganhan,tien_dnhm,tien_tt, ngay_tt, soseri, dthu_goi, dthu_ps,
            doanhthu_dongia_nvptm, luong_dongia_nvptm,  doanhthu_kpi_nvptm, 
            luong_dongia_dnhm_nvptm, thang_tldg_dnhm
    from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm = 202405 and thoihan_id=1
            and dthu_goi<>dthu_ps;
   




