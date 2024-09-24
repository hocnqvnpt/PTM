--file nay chay 2 dot, full code
		/*Danh sach cac he so
		heso_dichvu, heso_dichvu_1, phanloai_kh, heso_khachhang, heso_tbnganhan, heso_tratruoc
		, heso_khuyenkhich, heso_kvdacthu, heso_vtcv_nvptm, heso_vtcv_dai, heso_vtcv_nvhotro
		, heso_hotro_nvptm, heso_hotro_dai, heso_hotro_nvhotro, heso_quydinh_nvptm, heso_quydinh_dai
		, heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_hoso, heso_dichvu_dnhm, dongia
		*/
-- Kiem tra luu bang:
select * from ttkd_bsc.ct_bsc_ptm_202403;
create table ttkd_bsc.ct_bsc_ptm_202403 as
	select * from ttkd_bsc.ct_bsc_ptm where thang_ptm >= 202204 		--thang n-24
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
				   where sudung=1 and not exists (select * from ttkd_bsc.dm_loaihinh_hsqd where loaitb_id=a.loaitb_id)
				   ;
	   
	  -- select * from ttkd_bsc.dm_loaihinh_hsqd where ngay_cn is not null order by ngay_cn desc;
        
        ---da xong 22g42 ngay 14/5/24
-- Tien thanh toan hop dong dau vao:  (thang 6 ktra lai quet tren DATAGURAD day du, TTKDDB do dong bo
		select * from hocnq_ttkd.temp_tt
					where ma_gd in ('HCM-LD/01479990',
													'HCM-LD/01424347',
													'HCM-LD/01477643')
				;
		
		drop table hocnq_ttkd.temp_tt1 purge
		;
		create table hocnq_ttkd.temp_tt as
			select * from ttkd_hcm.temp_tt@dataguard
			;
		
		create table hocnq_ttkd.temp_tt1 as
				select kh.loaihd_id, a.ma_gd, a.hdkh_id, b.hdtb_id,a.ngay_cn, a.ngay_tt ngay_tt, a.soseri, sum(b.tien) tien, a.trangthai trangthai
				    from css_hcm.hd_khachhang kh
								, css_hcm.hd_thuebao tb
						  , css_hcm.phieutt_hd a
						 , css_hcm.ct_phieutt b
				    where kh.hdkh_id = tb.hdkh_id and tb.hdtb_id = b.hdtb_id and a.phieutt_id = b.phieutt_id and a.trangthai = 1 --and b.tien >0 
										and tb.tthd_id = 6
							--	and to_number(to_char(a.ngay_cn,'yyyymm'))>=202401					--thang n -3
								and exists (select 1 from ttkd_bsc.ct_bsc_ptm 
															where thuebao_id = tb.thuebao_id and nvl(trangthai_tt_id,0)=0 
																		and thang_ptm >= 202401	---thang n-3
													)
				    group by kh.loaihd_id, a.ma_gd, a.hdkh_id, b.hdtb_id,a.ngay_cn, a.ngay_tt, a.soseri, a.trangthai
				    ;
			create index hocnq_ttkd.temp_tt_hdtbid on hocnq_ttkd.temp_tt1 (hdtb_id)
			;

		
			MERGE INTO ttkd_bsc.ct_bsc_ptm a
			USING (select ngay_tt, soseri, tien, trangthai, hdtb_id from hocnq_ttkd.temp_tt1) b
			ON (b.hdtb_id = a.hdtb_id)
			WHEN MATCHED THEN
					update SET ngay_tt = b.ngay_tt, soseri = b.soseri , tien_tt = b.tien, trangthai_tt_id = b.trangthai
---			select * from ttkd_bsc.ct_bsc_ptm a
			WHERE thang_ptm >= 202401 and chuquan_id<>264 and thang_tldg_dt is null and nvl(trangthai_tt_id,0)=0			--thang n -3
								and exists(select 1 from hocnq_ttkd.temp_tt1 where hdtb_id = a.hdtb_id)
--								and ma_gd in ('HCM-LD/01479990',
--'HCM-LD/01424347',
--'HCM-LD/01477643')
				--	and nvl(trangthai_tt_id,0)=0
			;
			
commit;
                                                                     
-- DTHU_PS:
-- Dot 1:
		drop table ct_no_20240401 purge;
		;
		create table ttkd_bsc.ct_no_20240401 as select * from bcss.v_ct_no@dataguard where phanvung_id=28 and ky_cuoc=20240401;
		create index ct_no_20240401_thuebaoid on ttkd_bsc.ct_no_20240401 (thuebao_id);
   
-- dthu_ps thang n:
		update ct_bsc_ptm a 
			   set dthu_ps = (select sum(nogoc) from ttkd_bsc.ct_no_20240401
								    where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
		    where thang_ptm=202404 and loaitb_id<>21 and dthu_ps is null 
			   and exists (select 1 from ttkd_bsc.ct_no_20240401
								where nogoc>0 and khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
		;
     
		update ct_bsc_ptm a 
			   set dthu_ps_n1 = (select sum(nogoc) from ttkd_bsc.ct_no_20240401
										 where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
		    where thang_ptm=202403 and loaitb_id not in (20,21,131)
			   and dthu_ps_n1 is null
			   and exists (select 1 from ttkd_bsc.ct_no_20240401
							   where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
					   ;
            
			update ct_bsc_ptm a 
				   set dthu_ps_n2 = (select sum(nogoc) from ttkd_bsc.ct_no_20240401
											 where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
			    where thang_ptm=202402 and loaitb_id not in (20,21,131)
				   and dthu_ps_n2 is null
				   and exists (select 1 from ttkd_bsc.ct_no_20240401
				   where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
			;

                            
			update ct_bsc_ptm a 
				   set dthu_ps_n3 = (select sum(nogoc) from ttkd_bsc.ct_no_20240401
											 where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
			    where thang_ptm=202401 and loaitb_id not in (20,21,131)
				   and dthu_ps_n3 is null
				   and exists (select 1 from ttkd_bsc.ct_no_20240401
								   where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id);          

                       
-- Dot 2: 
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps = (select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd where ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
		    -- select * from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm=202404 and loaitb_id<>21 and dthu_ps is null 
			   and exists(select 1 from ttkd_bct.cuoc_thuebao_ttkd where dthu>0 and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
             ;   
                            
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps_n1 = (select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd where ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
		    where thang_ptm=202403 and loaitb_id<>21 and dthu_ps_n1 is null
			   and exists(select 1 from ttkd_bct.cuoc_thuebao_ttkd where dthu>0 and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id);
                
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps_n2 = (select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd where ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
		    where thang_ptm=202402 and loaitb_id<>21 and dthu_ps_n2 is null
			   and exists(select 1 from ttkd_bct.cuoc_thuebao_ttkd where dthu>0 and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
			   ;
                            
			update ttkd_bsc.ct_bsc_ptm a 
				   set dthu_ps_n3 = (select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd where ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
			    where thang_ptm=202401 and loaitb_id<>21 and dthu_ps_n3 is null 
				   and exists(select 1 from ttkd_bct.cuoc_thuebao_ttkd where dthu>0 and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
				   ;


-- DTHU GOI TB NGAN HAN THANG n:
		update ttkd_bsc.ct_bsc_ptm a 
			   set  ngay_cat = (select ngay_cat from css_hcm.db_thuebao where thuebao_id=a.thuebao_id)
					 ,trangthaitb_id = (select trangthaitb_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id)
					 ,songay_sd = (select (case when db.trangthaitb_id in (5,6) and to_char(db.ngay_td,'yyyymm')>=to_char(db.ngay_sd,'yyyymm') then trunc(db.ngay_td)-trunc(db.ngay_sd)+1
														  when db.trangthaitb_id in (7) and to_char(db.ngay_cat,'yyyymm')>=to_char(db.ngay_sd,'yyyymm') then trunc(db.ngay_cat)-trunc(db.ngay_sd)+1
														  when db.trangthaitb_id not in (5,6,7) and thoihan_id=1 then trunc(db.tg_thue_den)-trunc(db.ngay_sd)+1
														  else null end)
										from css_hcm.db_thuebao db
										where db.thuebao_id=a.thuebao_id)
					 ,dthu_goi=nvl(a.dthu_ps,0)+nvl(a.dthu_ps_n1,0)+nvl(a.dthu_ps_n2,0)+nvl(a.dthu_ps_n3,0)
					 ,thang_luong=1                                      
		    -- select ma_tb, trangthaitb_id, ngay_bbbg, ngay_cat, (select ngay_cat from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) ngay_cat_dbonline, dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3, dthu_goi, nvl(a.dthu_ps,0)+nvl(a.dthu_ps_n1,0)+nvl(a.dthu_ps_n2,0)+nvl(a.dthu_ps_n3,0) dthu_goi_new, ghi_chu from ct_bsc_ptm a
		    where thang_ptm>=202401 and thoihan_id=1 and dichvuvt_id in (1,10,11,4,7,8,9) ---thang n-3
					   and (thang_tldg_dt is null or thang_tldg_dt=202404)											---thang n
					   and nvl(a.dthu_ps,0)+nvl(a.dthu_ps_n1,0)+nvl(a.dthu_ps_n2,0)+nvl(a.dthu_ps_n3,0)>nvl(dthu_goi,0)
					   and exists(select 1 from css_hcm.db_thuebao where trangthaitb_id=7 and to_number(to_char(ngay_cat,'yyyymm'))=202404 and thuebao_id=a.thuebao_id)  --thang n
			   ;      
            
  
-- Tinh lai dthu goi cho CA, IVAN, HDDT, TAX, VNPT HKD chua co dthu_goi tai thang_ptm:
			----Tbao DTHU_goi = 0, nhung tong dthu_ps >0 --> UPDATE DATCOC_CSD into DTHU_GOI
			----Ap dung dich vu  thang toan 1 lan
				drop table hocnq_ttkd.temp_ps purge
				;
				select * from hocnq_ttkd.temp_ps
				;
				create table hocnq_ttkd.temp_ps as;
--insert into hocnq_ttkd.temp_ps
								select thang_ptm, thuebao_id, ma_gd, ma_tb, dthu_goi, dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3
										  , nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0) dthu_tong
										  , (select sum(nogoc) from ttkd_bsc.ct_no_20240401
											 where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953)
													   and thuebao_id=a.thuebao_id) dthu_ps_n
										  ,(select round(cuoc_dc/1.1,0) from css_hcm.db_datcoc where hieuluc=1 and ttdc_id = 0 and to_number(to_char(ngay_cn,'yyyymm'))>=202401 and thuebao_id=a.thuebao_id) datcoc_csd_new
										  ,(select (months_between( to_date(least(thang_kt,nvl(thang_kt_dc,'999999'),nvl(thang_huy,'999999')) , 'yyyymm') , 
																					    to_date(thang_bd,'yyyymm'))+1)
												 from css_hcm.db_datcoc where hieuluc=1 and ttdc_id = 0 and to_char(ngay_cn,'yyyymm')='202404' and thuebao_id=a.thuebao_id) sothang_dc_new
										 ,(select thang_bd from css_hcm.db_datcoc where hieuluc=1 and ttdc_id = 0 and to_number(to_char(ngay_cn,'yyyymm'))>=202401 and thuebao_id=a.thuebao_id) thang_bddc_new
										 ,thang_tldg_dt, thang_tlkpi_to, thang_tlkpi_phong, lydo_khongtinh_dongia
								from ttkd_bsc.ct_bsc_ptm a
								where thang_ptm >= 202401 and nvl(dthu_goi, 0) = 0 --dthu_goi is null		---thang n -3
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
								   dthu_goi_goc = (select datcoc_csd_new from hocnq_ttkd.temp_ps where dthu_tong>0 and ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm),
								   dthu_goi = (select datcoc_csd_new from hocnq_ttkd.temp_ps where dthu_tong>0 and ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm), 
								   datcoc_csd= (select datcoc_csd_new from hocnq_ttkd.temp_ps where dthu_tong>0 and ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm),
								   thang_bddc= (select thang_bddc_new from hocnq_ttkd.temp_ps where dthu_tong>0 and ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm), 
								   sothang_dc= (select sothang_dc_new from hocnq_ttkd.temp_ps where dthu_tong>0 and ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm), 
								   thang_tldg_dt='', thang_tlkpi='', thang_tlkpi_to='', thang_tlkpi_phong=''      
				-- select thang_luong, hdkh_id, ma_tb,  dich_vu, tenkieu_ld, dthu_goi_goc, dthu_goi, thang_ptm, dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3 , lydo_khongtinh_dongia, doanhthu_dongia_nvptm, luong_dongia_nvptm, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to from ttkd_bsc.ct_bsc_ptm a
				where exists  (select dthu_tong from hocnq_ttkd.temp_ps where dthu_tong>0 and ma_gd=a.ma_gd and ma_tb =a.ma_tb and thang_ptm=a.thang_ptm)
						   and 
						   nvl(dthu_goi, 0) = 0 --dthu_goi is null
--						and ma_tb = 'hcm_ca_00067906'
;
commit;
rollback;

select thang_luong, thuebao_id, ma_tb,  dich_vu, tenkieu_ld, datcoc_csd, dthu_goi_goc, dthu_goi, thang_ptm, dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3
			, lydo_khongtinh_dongia, doanhthu_dongia_nvptm, luong_dongia_nvptm, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to 
from ttkd_bsc.ct_bsc_ptm a
     where thang_luong='2' ;
     
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
		drop table ttkd_bsc.bsc_smsbrn_202404 purge
		;
		create table ttkd_bsc.bsc_smsbrn_202404 as
		    select thang_ptm, ma_gd, ma_tb, thuebao_id, dthu_ps, dthu_ps_n1,
				  dthu_goi dthu_goi_n, dthu_goi_ngoaimang dthu_goi_ngoaimang_n,  
				  cast(null as number(12)) dthu_goi_n1, cast(null as number(12)) dthu_goi_ngoaimang_n1,
				  cast(null as number(12)) dthu_goi_bq, cast(null as number(12)) dthu_goi_ngoaimang_bq            
		    from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm=202403 and loaitb_id=131 ---thang n -1
		    ;


--update bsc_smsbrn_202403 a 
--    set dthu_goi_ngoaimang_n = (select sum(nogoc) from ttkd_bsc.v_tonghop  
--                                                            where ky_cuoc=20240201 and khoanmuctc_id in (37,38,39,40,935,936,938,939,943,944,945,4097) and ma_tb=a.ma_tb)
--        ,dthu_goi_ngoaimang_n1= (select sum(nogoc) from ttkd_bsc.v_tonghop  
--                                                            where ky_cuoc=20240301 and khoanmuctc_id in (37,38,39,40,935,936,938,939,943,944,945,4097) and ma_tb=a.ma_tb);
		update ttkd_bsc.bsc_smsbrn_202404 a 
			    set dthu_goi_ngoaimang_n = (select sum(nogoc) from bcss.v_tonghop@dataguard  --thang n- 1
															where phanvung_id=28 and ky_cuoc=20240301 and khoanmuctc_id in (37,38,39,40,935,936,938,939,943,944,945,4097) and ma_tb=a.ma_tb)
				   ,dthu_goi_ngoaimang_n1= (select sum(nogoc) from bcss.v_tonghop@dataguard    ---thang n
                                                            where phanvung_id=28 and ky_cuoc=20240401 and khoanmuctc_id in (37,38,39,40,935,936,938,939,943,944,945,4097) and ma_tb=a.ma_tb)
			;
             
			update ttkd_bsc.bsc_smsbrn_202404 a 
			    set   dthu_goi_n=nvl(dthu_ps,0)-nvl(dthu_goi_ngoaimang_n,0)
					  , dthu_goi_n1=nvl(dthu_ps_n1,0)-nvl(dthu_goi_ngoaimang_n1,0)
			;


			update ttkd_bsc.bsc_smsbrn_202404 a 
			    set  dthu_goi_bq=round( (nvl(dthu_goi_n,0)+nvl(dthu_goi_n1,0) ) /2,0),
					  dthu_goi_ngoaimang_bq=round( (nvl(dthu_goi_ngoaimang_n,0)+nvl(dthu_goi_ngoaimang_n1,0) )/2,0)
					;

                               
			update ttkd_bsc.ct_bsc_ptm a 
			    set (dthu_goi_goc, dthu_goi, dthu_goi_ngoaimang, thang_luong)=
					  (select nvl(dthu_goi_bq,0)+nvl(dthu_goi_ngoaimang_bq,0) , dthu_goi_bq, dthu_goi_ngoaimang_bq, 202404		--thang n
						 from ttkd_bsc.bsc_smsbrn_202404
						 where thuebao_id=a.thuebao_id)
			    where thang_ptm=202403 and loaitb_id=131  --thang n-1
			    ;
            commit;
            rollback;

-- Doi tuong KH:  dot 2 khong xoa, nhung chay cac th doituong_kh = null xy ly tiep                      
		update ttkd_bsc.ct_bsc_ptm set doituong_kh=null 
	--		select * from ttkd_bsc.ct_bsc_ptm
		    where thang_ptm=202404 and (dichvuvt_id!=2 or (dichvuvt_id=2 and loaitb_id<>21) ) and ma_kh<>'GTGT rieng' 
		    ;
            

    -- dot 1 khi chua co file giao:     
			update ttkd_bsc.ct_bsc_ptm a 
				   set doituong_kh = (case when (select c.khdn from css_hcm.db_khachhang b, css_hcm.loai_kh c 
															    where b.loaikh_id=c.loaikh_id and khachhang_id=a.khachhang_id) =0 then 'KHCN' else 'KHDN' end)
			    --	    select doituong_kh from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202404 and ma_kh<>'GTGT rieng'  and doituong_kh is null 
				   and (dichvuvt_id !=2 or (dichvuvt_id=2 and loaitb_id<>21) ) 
				 
				   ;
           ---end dot 1, khong chay lai doan nay cho dot 2                     
              
    -- dot 2:                      
				update ttkd_bsc.ct_bsc_ptm a
					   set doituong_kh = (case when (loaitb_id not in (20,149) and doituong_id in (374, 387, 361, 362))
																    or (loaitb_id in (20,149) and doituong_id in (1,25))
													   then 'KHCN' else 'KHDN' end)
			--	    select * from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202404 and ma_kh<>'GTGT rieng' and doituong_kh is null
					   and (lydo_khongtinh_luong is null or lydo_khongtinh_luong not like '%Chu quan khong thuoc TTKD-HCM%')
					   ;
                            
                                            
				update ttkd_bsc.ct_bsc_ptm a 
					   set doituong_kh ='KHDN'            
				    where  thang_ptm=202404 and ma_kh='GTGT rieng' and doituong_kh is null
				    ;
             ---end dot 2
               update ttkd_bsc.ct_bsc_ptm a set doituong_kh='KHDN'
					    -- select chuquan_id, khachhang_id, ma_tb, ten_tb, doituong_kh from ttkd_bsc.ct_bsc_ptm a
					    where thang_ptm=202404 and doituong_kh='KHCN' 
						   and (bo_dau(upper(ten_tb)) like 'CONG TY%' or bo_dau(upper(ten_tb)) like 'CTY%'  
								 OR bo_dau(upper(ten_tb)) like 'CN TONG CONG TY%' or bo_dau(upper(ten_tb)) like 'CHI NHANH%'
								 OR bo_dau(upper(ten_tb)) like 'NGAN HANG%' or bo_dau(upper(ten_tb)) like 'CHI CUC %' 
								 OR bo_dau(upper(ten_tb)) like 'VPDD%' OR bo_dau(upper(ten_tb)) like 'VAN PHONG DAI DIEN%'
								 OR bo_dau(upper(ten_tb)) like '%TR__NG PH_ TH_NG TRUNG H_C%'
								 OR bo_dau(upper(ten_tb)) like '%TR__NG TRUNG H_C PH_ TH_NG%' ) 
			 ;
/* 
select thang_ptm, ma_tb, ten_tb, doituong_kh, loaitb_id from ct_bsc_ptm a
    where  thang_ptm=202403 and loaitb_id<>21 and doituong_kh is null;
                             
                   
select ma_gd, ma_tb, ten_tb, mst, ma_dt_kh, doituong_kh, (select c.khdn from css_hcm.db_khachhang b, css_hcm.loai_kh c where b.loaikh_id=c.loaikh_id and khachhang_id=a.khachhang_id) khdn
    from ttkd_bsc.ct_bsc_ptm a 
    --  update ct_bsc_ptm a set doituong_kh='KHDN'
    where thang_ptm=202403 and (dichvuvt_id!=2 or (dichvuvt_id=2 and loaitb_id<>21) ) and ma_kh<>'GTGT rieng' 
        and (lydo_khongtinh_luong is null or lydo_khongtinh_luong not like '%Chu quan khong thuoc TTKD-HCM%')
        and doituong_kh='KHCN' and ma_dt_kh='dn'
        and (select c.khdn from css_hcm.db_khachhang b, css_hcm.loai_kh c 
                where b.loaikh_id=c.loaikh_id and khachhang_id=a.khachhang_id) =1;
                                          
select ma_gd, ma_tb, ma_dt_kh, doituong_kh,
        (select c.khdn from css_hcm.db_khachhang b, css_hcm.loai_kh c where b.loaikh_id=c.loaikh_id and khachhang_id=a.khachhang_id) khdn, ten_tb
    from ttkd_bsc.ct_bsc_ptm a 
    where thang_ptm=202403 and (dichvuvt_id!=2 or (dichvuvt_id=2 and loaitb_id<>21) ) and ma_kh<>'GTGT rieng' 
        and (lydo_khongtinh_luong is null or lydo_khongtinh_luong not like '%Chu quan khong thuoc TTKD-HCM%')
        and doituong_kh='KHDN' and ma_dt_kh='cn'
        and (select c.khdn from css_hcm.db_khachhang b, css_hcm.loai_kh c where b.loaikh_id=c.loaikh_id and khachhang_id=a.khachhang_id) =0;

                                             

                                                       
                                    
select ma_gd, ma_tb, ten_tb, (select ten_kh from css_hcm.db_khachhang where khachhang_id=a.khachhang_id) ten_kh , mst, mst_tt,
        (select ten_dt from css_hcm.doituong where doituong_id=a.doituong_id) doituong,
        doituong_kh, (select c.khdn from css_hcm.db_khachhang b, css_hcm.loai_kh c where b.loaikh_id=c.loaikh_id and khachhang_id=a.khachhang_id) khdn ,
        dich_vu, loaitb_id,   so_gt, mst, khachhang_id
    from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm=202403 and loaitb_id not in (21,116,117) and ma_kh<>'GTGT rieng' and loaitb_id not in (288,116)
        and doituong_kh='KHDN' and upper(ten_tb) not like '%TNHH%' and upper(ten_tb) not like '%THPT%'
        and upper(ten_tb) not like '%C_NG TY%' and upper(ten_tb) not like '%CTY%' and upper(ten_tb) not like '%NGÂN HÀNG%'
        and upper(ten_tb) not like '%TRUNG T_M%' and upper(ten_tb) not like '%VP_D%' and upper(ten_tb) not like '%VNPT%'
        and upper(ten_tb) not like '%VI_T NAM%' and upper(ten_tb) not like '%V_N PH_NG __I DI_N%' 
        and upper(ten_tb) not like 'B_NH VI_N%' and upper(ten_tb) not like 'TR__NG M_M NON %' and upper(ten_tb) not like 'CONG TY%' 
        and upper(ten_tb) not like 'DOANH NGHI_P %' and upper(ten_tb) not like 'H_P T_C X_%' 
        and upper(ten_tb) not like 'LI_N HI_P%' and upper(ten_tb) not like 'NG_N H_NG %' 
        and upper(ten_tb) not like '_Y BAN NH%' 
        and upper(ten_tb) not like 'TR__NG THCS%' and upper(ten_tb) not like 'TR__NG TI_U H_C%' 
        and upper(ten_tb) not like 'DNTN %' and upper(ten_tb) not like 'CHI NH_NH %'  and upper(ten_tb) not like '_Y BAN%'  ;
   
        update ct_bsc_ptm set doituong_kh='KHCN' 
        where thang_ptm=202403 and ma_tb in ('84942260492','84911263578','vanphonglynhon','tourbooking-2022');
                                
*/
commit;

-- QLDA: xoa , chay lai
delete from ttkd_bsc.ct_bsc_ptm_kiemtraduan where thang_ptm=202404 and  ma_gd = '00798153'
		;
		select * from ttkd_bsc.ct_bsc_ptm_kiemtraduan where ma_gd = '00798153';
		
			insert into ttkd_bsc.ct_bsc_ptm_kiemtraduan
								(ptm_id, thang_ptm, ma_gd, ma_tb, dich_vu, ma_duan_banhang, mst, mst_tt
								, dichvuvt_id, loaitb_id, duan_id, duan_daduyet, duan_mst, kt_loaitb_id, kt_mst 
								)
			    select a.id, a.thang_ptm, a.ma_gd, a.ma_tb, a.dich_vu, a.ma_duan_banhang, a.mst, a.mst_tt, a.dichvuvt_id, a.loaitb_id, b.ma_yeucau, b.daduyet, b.masothue
						 ,(case when exists (select 1 from ttkdhcm_ktnv.amas_yeucau_dichvu c, ttkdhcm_ktnv.amas_loaihinh_tb d
													  where c.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', '')) 
															and c.ma_dichvu=d.loaitb_id and d.loaitb_id_obss=a.loaitb_id) then 1 
							    else case when exists(select 1 from ttkd_bsc.map_loaihinhtb where loaitb_id=a.loaitb_id and loaitb_id_qlda=b.ma_dichvu) 
											    then 1 else 0 end 
							    end ) kt_loaitb_id
						 ,(case when regexp_replace (mst, '\D', '')=regexp_replace (b.masothue, '\D', '') or regexp_replace (mst_tt, '\D', '')=regexp_replace (b.masothue, '\D', '') 
							    then 1 else 0 end ) kt_mst
			    from ttkd_bsc.ct_bsc_ptm a, ttkdhcm_ktnv.amas_yeucau b
			    where a.thang_ptm=202404 and loaihd_id<>31 and thuebao_id is not null and a.doituong_kh='KHDN' and nhom_tiepthi<>2
			--	   and a.duan_id = b.ma_yeucau(+) 
						and  to_number(regexp_replace (a.ma_duan_banhang, '\D', '')) = b.ma_yeucau (+)
					
				
				; 
				commit;
        
        
-- ket qua kiem tra du an: neu khong hop le thi giam dthu quy doi 50%
			update ttkd_bsc.ct_bsc_ptm_kiemtraduan a
			set kiemtra_duan = (
											with kq as (
													  select ptm_id, ma_duan_banhang, duan_id, duan_daduyet, kt_mst, kt_loaitb_id
																   , case when ma_duan_banhang is null then '; OneBSS khong nhap ma du an' end kq1
																   , case when ma_duan_banhang is not null and duan_id is null then '; Ma du an '||ma_duan_banhang||' chua dang ky tren QLDA' end kq2
																   , case when duan_id is not null and (duan_daduyet is null or duan_daduyet <> 1) then '; Du an chua duoc duyet' end kq3
																   , case when duan_id is not null and kt_mst=0 then '; Khong dung KH (mst)' end kq4																  
																   , case when duan_id is not null and nvl(kt_loaitb_id, 0)=0 then '; Khong dung dich vu dang ky' end kq5
													  from ttkd_bsc.ct_bsc_ptm_kiemtraduan
													  where thang_ptm=202404
													)                                        
									  select nvl(c.kq1,'') || nvl(c.kq2,'') || nvl(c.kq3,'') || nvl(c.kq4,'') || nvl(c.kq5,'') 
									  from  kq c
									  where c.ptm_id = a.ptm_id
								) 
			where thang_ptm=202404 	
			;


			update ttkd_bsc.ct_bsc_ptm a 
				   set kiemtra_duan = '' 
		--- select kiemtra_duan from ttkd_bsc.ct_bsc_ptm a 
			    where thang_ptm=202404 and doituong_kh='KHDN' and kiemtra_duan is not null
			 
			    ;
    
			update ttkd_bsc.ct_bsc_ptm a 
				   set kiemtra_duan = (select kiemtra_duan from ttkd_bsc.ct_bsc_ptm_kiemtraduan where thang_ptm=202404 and ptm_id=a.id)
		--- select kiemtra_duan from ttkd_bsc.ct_bsc_ptm a 
			    where exists(select 1 from ttkd_bsc.ct_bsc_ptm_kiemtraduan where thang_ptm=202404 and ptm_id=a.id)
						
			    ;

		commit;

/* 
select * from ct_bsc_ptm a
    where a.thang_ptm=202403 and loaitb_id not in (20,21,210) 
        and duan_id is not null and kiemtra_duan is not null;
*/   

          
-- KH hien huu - KH moi:   
			---dot 2  khong xoa
			update ttkd_bsc.ct_bsc_ptm set khhh_khm='' 			
			    where thang_ptm=202404 and khhh_khm is not null
			    ;
                                            
                  --chay lai                          
			update ttkd_bsc.ct_bsc_ptm set khhh_khm='KHM' 
			    where thang_ptm=202404
				   and nguon='web Digishop' 
						 or (doituong_kh='KHCN'
							    and (dichvuvt_id!=2 or (dichvuvt_id=2 and loaitb_id<>21) or dichvuvt_id is null)
							    and nguon in ('ptm_codinh_202404','ccq_202404','dt_ptm_vnp_202404','shop_hcm_mytv_online_202404'))
				   and khhh_khm is null
				   ; 
  

			update ttkd_bsc.ct_bsc_ptm a 
			    set khhh_khm='KHHH'
			    where thang_ptm=202404 and khhh_khm is null 
				   and (nguon in ('thaydoitocdo_202404','tailap_202404') or loaihd_id<>1 ) 
				   ;
     ----chua chay trong dot 1              
				---chay hoi lau ---toi uu lai
			update ttkd_bsc.ct_bsc_ptm a 
				   set khhh_khm = 'KHHH'
			        -- select count(*) from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202404 and doituong_kh='KHDN' and mst is not null --thang n
					  and khhh_khm is null
					  and exists (select 1 from ttkd_bct.db_thuebao_ttkd
												   where ma_dt_kh='dn' and cvnv is null and tb_dacbiet is null 
																  and trangthaitb_id not in (7,8,9) and to_number(to_char(ngay_sd,'yyyymm')) <202404			--- thang n
																  and to_number(regexp_replace (mst, '\D', ''))=to_number(regexp_replace (a.mst, '\D', ''))
											)
					  ;
                                              
					update ttkd_bsc.ct_bsc_ptm a 
					   set khhh_khm =  'KHHH'
					     -- select count(*) from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202404 and doituong_kh='KHDN'
						  and khhh_khm is null and nvl(loaitb_id, 21) !=21
				---hoac 1
						  and  exists (select 1 from ttkd_bct.db_thuebao_ttkd
															   where  ma_dt_kh='dn' and cvnv is null and tb_dacbiet is null 
																			  and trangthaitb_id not in (7,8,9) and to_number(to_char(ngay_sd,'yyyymm'))<202404
																			  and to_number(regexp_replace (mst, '\D', ''))=to_number(regexp_replace (a.mst, '\D', ''))
													)
				---hoac 2
--						and   exists (select 1 from ttkd_bct.db_thuebao_ttkd
--															   where ma_dt_kh='dn' and cvnv is null and tb_dacbiet is null                                                                         
--																  and trangthaitb_id not in (7,8,9) and to_number(to_char(ngay_sd,'yyyymm'))<202404
--																  and lower(so_gt)=lower(a.so_gt)
--												  )
			---hoac 3
--						and  exists (select 1 from ttkd_bct.db_thuebao_ttkd
--										   where ma_dt_kh='dn' and cvnv is null and tb_dacbiet is null 
--											  and trangthaitb_id not in (7,8,9) and to_number(to_char(ngay_sd,'yyyymm'))<202404
--											  and khachhang_id=a.khachhang_id
--											  )          
					;
					
					update ttkd_bsc.ct_bsc_ptm a 
					   set khhh_khm =  'KHH'
					     -- select count(*) from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202404 and doituong_kh='KHDN'
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
			update ttkd_bsc.ct_bsc_ptm a set diaban =null
	---	select diaban from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng')
			    	
			    ;
                
          
				update  ttkd_bsc.ct_bsc_ptm a
				    set diaban = (case when nhom_tiepthi=2 or thuebao_id is null then 'Khong xet trong/ngoai CT ban hang'
											  when ma_vtcv not in ('TTKD_CTV_KD_16','TTKD_CTV_KD_02','VNP-HNHCM_BHKV_6','VNP-HNHCM_BHKV_6.1',
																		    'VNP-HNHCM_BHKV_17','VNP-HNHCM_BHKV_42','VNP-HNHCM_KHDN_3','VNP-HNHCM_KHDN_3.1',
																		    'TTKD_CTV_KD_08','VNP-HNHCM_KHDN_4','VNP-HNHCM_BHKV_41',
																		    'VNP-HNHCM_KHDN_18', 'VNP-HNHCM_KTNV_8') then 'Khong xet trong/ngoai CT ban hang'
											  when doituong_kh='KHCN' then 'Trong CT ban hang'
											  when doituong_kh='KHDN' and kiemtra_duan is null then 'Trong CT ban hang'
											  when doituong_kh='KHDN' and kiemtra_duan is not null then 'Ngoai CT ban hang'
											  else null end)
				--			select diaban from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng')
					   and ma_pb is not null and diaban is null 
					   --- select kiemtra_duan, diaban from ttkd_bsc.ct_bsc_ptm a 
					   	--and a.ma_gd = '00798153'
					   ;
  

/*
select nguon, doituong_kh, diaban , count(*)
    from ct_bsc_ptm a
    where thang_ptm=202404 and loaitb_id<>21 
        and  (lydo_khongtinh_luong not like '%Chu quan khong thuoc TTKD-HCM%' or lydo_khongtinh_luong is null)
        and manv_ptm is not null  
    group by doituong_kh, diaban, nguon
    order by doituong_kh, diaban, nguon;
 */


-- Xet mien HS goc doi voi hop dong chuyen doi, nang cap, va lap moi cdbr/tsl co tra truoc      
			update ttkd_bsc.ct_bsc_ptm set mien_hsgoc='' 
			    where thang_ptm=202404 and dichvuvt_id not in (2,13,14,15,16) and loaitb_id not in (21,172) and mien_hsgoc is not null
			    ;
                               
			update ttkd_bsc.ct_bsc_ptm a 
				   set mien_hsgoc=1 
			    where thang_ptm=202404 and mien_hsgoc is null 
				   and ( (dichvuvt_id not in (2,13,14,15,16) and (loaihd_id is null or chuquan_id in (266,264)))
							or (thang_ptm=202404 and dichvuvt_id=4 and sothang_dc>=6)                                   -- Fiber, MyTV dong truoc truoc >=6 thang  
							or ( thang_ptm=202404 and loaitb_id in (15,17) and thuebao_cha_id is not null)       -- isdn, thue bao luong (so con)
							or nguon not in ('ptm_codinh_202404','ccq_202404','dt_ptm_vnp_202404') )
							;
                            
 

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
                                                    then decode(to_number(regexp_replace (goi_cuoc, '\D', '')) , 0,0.2, 1,0.2, 2,0.2, 3,0.2, 4,0.2, 5,0.2, 6,0.3, 7,0.3, 8,0.3, 9,0.3, 10,0.3, 11,0.3, 12,0.4, 18,0.4, null) 
                                            when loaitb_id=200 then    -- Ecabinet
                                                case when sothang_dc<3 or sothang_dc is null then 0.1
                                                         when sothang_dc>=3 and sothang_dc<6 then 0.2
                                                         when sothang_dc>=6 and sothang_dc<12 then 0.25
                                                         when sothang_dc>=12 then 0.35 else null 
                                                end
                                            when loaitb_id in (35,121,120) then      -- eGov (VNPT iGate (121), iOffice (35), VNPT Portal (120), ...) : hop dong theo thang (hinh thuc cho thue) => hsdv =1;  (hinh thuc tron goi => hsdv = 0.4
                                                case when nvl(datcoc_csd,0)>0 
                                                                        and (select nvl(muccuoc_tb,0) from ttkd_bct.ptm_codinh_202404 where thuebao_id=a.thuebao_id and hdtb_id=a.hdtb_id)>0                                                
                                                                then 0.4  -- thue thang , nhung co dat coc => tinh nhu dthu tron hop dong
                                                         when nvl(datcoc_csd,0)=0
                                                                        and (select nvl(muccuoc_tb,0) from ttkd_bct.ptm_codinh_202404 where thuebao_id=a.thuebao_id and hdtb_id=a.hdtb_id)>0                                     
                                                                then 1        
                                                         when (select nvl(muccuoc_tb,0) from ttkd_bct.ptm_codinh_202404 where thuebao_id=a.thuebao_id and hdtb_id=a.hdtb_id)=0
                                                                then 0.4  -- dthu tron hop dong 
                                                    end
                                                when loaitb_id in (11,58,61,210) and kieuld_id=96 then 0.3      -- tai lap
                                                when loaitb_id=153 and loaihd_id=41 then 
                                                    case when sothang_dc>=6 then 0.3 else 0 end                     -- Gia han VNPT SmartCloud theo VB 167/TTr-NS-DH 23/05/2022: chi ghi nhan khi gia han goi 6 thang tro len
                                                when loaitb_id=296 then                                                                -- VNPT Home-Clinic: theo thang : 1, theo gói 6t,12t: 0.3 , VB 328/TTKD HCM-DH 31/12/2021
                                                    case when sothang_dc>=6 then 0.3 else 1 end
                                                when loaitb_id in (279, 317, 287) then                                         -- VNPT AntiDDoS: theo thang =1, theo thuê dich vu 72 gio = 0.3, eoffice 718660 
                                                                                                                                                                    -- VNPT IOC (Trung tam Dieu hanh thong minh), VNPT eDIG (Phan mem He thong quan ly Ho so)
                                                    case when exists (select muccuoc_tb, datcoc_csd from ttkd_bct.ptm_codinh_202404
                                                                            where loaitb_id=279 and nvl(muccuoc_tb,0)=0 and datcoc_csd>0 
                                                                                and thuebao_id=a.thuebao_id and hdtb_id=a.hdtb_id) 
                                                                then 0.3 else (select heso_dichvu from ttkd_bsc.dm_loaihinh_hsqd b where a.loaitb_id=b.loaitb_id)
                                                     end
                                                when loaitb_id in (317,287,285,279) then                                            --  VNPT AntiDDoS ,VNPT IOC, VNPT eDIG, VNPT AI Camera
                                                     case when not exists (select 1 from ttkd_bct.ptm_codinh_202404 where nvl(muccuoc_tb,0)=0 and hdtb_id=a.hdtb_id)
                                                        then 1 else (select heso_dichvu from ttkd_bsc.dm_loaihinh_hsqd b where a.loaitb_id=b.loaitb_id) end     -- -- thue phan mem theo thang: 1 ; Mua tron goi phan mem: 0.3
                                     
                                            else (select heso_dichvu from ttkd_bsc.dm_loaihinh_hsqd b where a.loaitb_id=b.loaitb_id)                       
											 end
						  ,heso_dichvu_1 = case when loaitb_id=131 then 0.004 end  
				    -- select * from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202404 and loaitb_id not in (20,21,149) and nguon not like '%co che%' and id_447 is null and heso_dichvu is null
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
			select * from ttkd_bsc.dm_duan_dacthu where thang='202404'
			;
			
			insert into ttkd_bsc.dm_duan_dacthu 
					    select ma_da, ten_duan, heso_kvdacthu, '202404', ghichu , toanha_id
					    from ttkd_bsc.dm_duan_dacthu a
					    where not exists (select 1 from ttkd_bsc.dm_duan_dacthu where thang='202404' and ma_da=a.ma_da)    
							  and to_number(thang)=202403
							  ;

    
				update ttkd_bsc.ct_bsc_ptm a 
				    set heso_kvdacthu=null
			---	select heso_kvdacthu from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202404 and dichvuvt_id!=2
					   and ma_da is not null and heso_kvdacthu is not null
					   and exists (select 1 from ttkd_bsc.dm_duan_dacthu where thang='202404' and ma_da=a.ma_da)
					   ;
            
            
			update ttkd_bsc.ct_bsc_ptm a 
			    set heso_kvdacthu = (select heso_kvdacthu from ttkd_bsc.dm_duan_dacthu where thang='202404' and ma_da=a.ma_da)
			---	select heso_kvdacthu from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202404 
				   and ma_da is not null and heso_kvdacthu is null
				   and exists (select 1 from ttkd_bsc.dm_duan_dacthu where thang='202404' and ma_da=a.ma_da)
				   ;
                              

-- Shop / myvnpt / ctvxhh
-- Dot 1 ngay 5: chay het 2 dot
			update ttkd_bsc.ct_bsc_ptm a 
			    set  ghi_chu = (select nguoi_cn_goc from ttkd_bct.ptm_codinh_202404
									  where nguoi_cn_goc in ('shop.vnpt.vn','freedoo','myvnpt','dhtt.mytv','ws_smes') and hdtb_id=a.hdtb_id) || decode(ghi_chu,null,null,' - ') || ghi_chu ,
					  heso_kvdacthu = 0.5
		---	select heso_kvdacthu from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202404 and ma_pb='VNP0703000'      
				   and exists (select 1 from ttkd_bct.ptm_codinh_202404
								   where nguoi_cn_goc in ('shop.vnpt.vn','freedoo','myvnpt','dhtt.mytv','ws_smes') and hdtb_id=a.hdtb_id)
			;
                                    
   
   
-- Dot 2 : chay het 2 dot   
				update ttkd_bsc.ct_bsc_ptm a 
				    set heso_kvdacthu= (case -- when exists (select 1 from dulieu_ftp.hcm_br_online_202404@vinadata where ma_gd=a.ma_gd and ma_tb=a.ma_tb and mahrm=a.manv_ptm) then 1  
														  when exists(select 1 from ttkd_bsc.digishop a 
																			 where thang=202404 and lower(trangthai_shop) like 'th_nh c_ng' and ma_gioithieu=a.manv_ptm) 
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
				    where thang_ptm=202404 and ma_pb='VNP0703000' 
					   and (ungdung_id is not null
							 or exists(select 1 from ttkd_bsc.digishop a 
										  where thang=202404 and lower(trangthai_shop) like 'th_nh c_ng' and ma_gioithieu=a.manv_ptm) 
							  or exists (select 1 from ttkd_bct.ptm_codinh_202404 
											 where nguoi_cn_goc in ('shop.vnpt.vn','freedoo','myvnpt','dhtt.mytv','shop.vnpt.vn','ws_smes') and thuebao_id=a.thuebao_id))
				;


-- He so tra truoc: 
		update ct_bsc_ptm a set heso_tratruoc=null 
		    where thang_ptm=202404 and loaitb_id!=20 and nguon not in ('Trong co che tinh luong','Ngoai co che tinh luong')  --loai tru cac file tu excel A Nghia
			   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id)
			   ;
 
            
			update ct_bsc_ptm a 
			    set heso_tratruoc = (case when sothang_dc>16 then 1.2
												   when sothang_dc>=12 and sothang_dc<=16 then 1.15
												   when sothang_dc>=6 and sothang_dc<12 then 1.1
												   when sothang_dc is not null and sothang_dc<6 then 1
												   else null end)
			---	select sothang_dc, heso_tratruoc from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202404 and sothang_dc is not null and loaitb_id!=20 and nguon not in ('Trong co che tinh luong','Ngoai co che tinh luong')
				   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id)
				   ;
   

---- IOffice: do tat ca hop dong ioffice deu tinh theo kieu ghi nhan 1 lan nen ko xet he so tra truoc 
--update ct_bsc_ptm a 
--    set heso_tratruoc=null
--    where thang_ptm=202403 and loaitb_id=35 and sothang_dc>0;
	chi Tung bao khong chay --4/5/24

  
    /*-- Kiem tra:
select dich_vu, sothang_dc, heso_tratruoc from ct_bsc_ptm a
    where thang_ptm=202403 and sothang_dc>=6
        and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id)
    group by dich_vu, sothang_dc, heso_tratruoc
    order by dich_vu, sothang_dc, heso_tratruoc;
*/
            
            
---- He so khuyen khich: 
--update ct_bsc_ptm 
--    set heso_khuyenkhich=null 
--    where thang_ptm=202403 ;
  
   
   
-- He so dai ly :  VB 353/TTr-DH-NS - 12/2023:    
			update ttkd_bsc.ct_bsc_ptm a 
			    set heso_daily=''
			    where thang_ptm=202404 and heso_daily is not null
			    ;
   
    
			update ttkd_bsc.ct_bsc_ptm a 
			    set heso_daily=0.05
		-- select ma_vtcv, ma_nguoigt, heso_daily from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202404 and manv_ptm is not null and heso_daily is null
				   and (upper(ma_tiepthi_new)like'GTGT_%' or upper(ma_tiepthi_new)like'DAILY_%' or upper(ma_tiepthi_new)like'DL_%'
									or upper(ma_nguoigt)like'GTGT_%' or upper(ma_nguoigt)like'DAILY_%' or upper(ma_nguoigt)like'DL_%'
						 )
				   and ma_vtcv not in ('VNP-HNHCM_KHDN_18.1','VNP-HNHCM_KHDN_3.1','VNP-HNHCM_KHDN_2','VNP-HNHCM_KHDN_1') 
				   and ma_nguoigt in ('GTGT00165','GTGT00166','GTGT00181','GTGT00182', 'GTGT00186','DL_HOANGKIM','GTGT00173','GTGT00164','GTGT00083','GTGT00125', 'GTGT00194')
				   ;
   
commit;
 
-- HE SO QUY DINH: 
			update ttkd_bsc.ct_bsc_ptm set heso_quydinh_nvptm=null
			    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') 
			    ;
                                

				update ttkd_bsc.ct_bsc_ptm a
				    set heso_quydinh_nvptm = case when chuquan_id=264 or ma_kh='GTGT rieng' or nhom_tiepthi=2 then 1
																  else case when diaban in ('Trong CT ban hang','Khong xet trong/ngoai CT ban hang') then 1
																				when diaban='Ngoai CT ban hang' then 0.5 else 1 end
														   end
						,heso_quydinh_nvhotro = case when manv_hotro is not null then 1 else null end
						,heso_quydinh_dai = case when manv_tt_dai is not null then 1 else null end
				
				  -- select heso_quydinh_nvptm, heso_quydinh_nvhotro, manv_hotro, heso_quydinh_dai, manv_tt_dai from ttkd_bsc.ct_bsc_ptm a    
				    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') --and heso_quydinh_nvhotro is null
				    --and a.ma_duan_banhang = '163041'
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
 
     
-- he so vtcv:          
				update ttkd_bsc.ct_bsc_ptm 
					   set heso_vtcv_nvptm='', heso_vtcv_dai='', heso_vtcv_nvhotro=''
				    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng')
				    ;
             
 
				update ttkd_bsc.ct_bsc_ptm a 
					   set   heso_vtcv_nvptm = 1
							 ,heso_vtcv_nvhotro = case when manv_hotro is not null then 1 else null end
							 ,heso_vtcv_dai = case when manv_tt_dai is not null then 1 else null end 
			--	    select heso_vtcv_nvptm, heso_vtcv_nvhotro, heso_vtcv_dai, manv_hotro from ttkd_bsc.ct_bsc_ptm a 
				    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') 
				    
				    
				    ;
             
    

-- He so khach hang: 
    -- cdbr+gtgt
			update ttkd_bsc.ct_bsc_ptm a set phanloai_kh=''
	---select phanloai_kh from ttkd_bsc.ct_bsc_ptm a 
			    where  thang_ptm=202404 and phanloai_kh is not null-- and thang_luong = 86
			    ; 
         
           
-- Da co file giao:        khong chay dot 1               
			update ttkd_bsc.ct_bsc_ptm a 
			    set phanloai_kh = case when ma_kh='GTGT rieng' then
													  (select (select ma_plkh from css_hcm.phanloai_kh where phanloaikh_id=b.plkh_id) 
														 from ttkd_bct.db_thuebao_ttkd b where to_number(regexp_replace(b.mst, '\D', '')) = to_number(regexp_replace(a.mst, '\D', ''))  and rownum=1)  
												when ma_kh<>'GTGT rieng' and loaitb_id not in (20, 21, 149)  
													  then (select distinct (select ma_plkh from css_hcm.phanloai_kh where phanloaikh_id=b.plkh_id ) 
															    from ttkd_bct.db_thuebao_ttkd b where b.thuebao_id=a.thuebao_id)
												when loaitb_id = 271 then 'DC2'
												when loaitb_id in (20,149) 
													  then (select (select ma_plkh from css_hcm.phanloai_kh where phanloaikh_id=b.plkh_id) 
															    from ttkd_bct.db_thuebao_ttkd b 
															    where loaitb_id=20 and b.ma_tb=a.ma_tb and to_char(ngay_sd,'dd/mm/yyyy')=to_char(a.ngay_bbbg,'dd/mm/yyyy'))
										 end
			-- select phanloai_kh, chuquan_id ,ma_kh, ma_tb, phanloai_kh from ttkd_bsc.ct_bsc_ptm a 

			    where thang_ptm=202404 and loaitb_id<>21 and phanloai_kh is null  --and a.ma_tb = '84853136618'
			    ;
                             commit;   
                                    rollback;
                                    
-- Chua co file giao (don gia ngay 5):     chay dot 1
			drop table hocnq_ttkd.temp_plkh purge;
			create table hocnq_ttkd.temp_plkh as
			    select distinct a.*, (select min(ma_plkh) from ttkd_bct.dbkh_plkh pl, css_hcm.phanloai_kh plkh 
														where pl.plkh_id=plkh.phanloaikh_id and ma_dt_kh='dn' and pl.khachhang_id=a.khachhang_id_plkh) phanloai_kh
				   from (select khachhang_id, (select max(khachhang_id) 
																		from ttkd_bct.db_thuebao_ttkd where trangthaitb_id not in (7,8,9) and ma_dt_kh='dn' and khachhang_id_goc=a.khachhang_id) khachhang_id_plkh
							 from ttkd_bsc.ct_bsc_ptm a
							 where thang_ptm=202404 and doituong_kh='KHDN' ) a
							 ;
             
				update ttkd_bsc.ct_bsc_ptm a 
				    set phanloai_kh = (case when exists(select 1 from ttkd_bct.dbkh_db where nhom in ('COOP','MAILINH') and regexp_replace(mst, '\D', '') = regexp_replace(a.mst, '\D', '') ) then 'DA2'  -- dbkh_db bang chi Nguyen
													 when loaitb_id=271 then 'DC2'
													 when ma_kh='GTGT rieng' 
														  then (select (select ma_plkh from css_hcm.phanloai_kh where phanloaikh_id=b.plkh_id) 
																    from ttkd_bct.db_thuebao_ttkd b where to_number(regexp_replace(b.mst, '\D', '')) = to_number(regexp_replace(a.mst, '\D', ''))  and rownum=1)   
													else (select phanloai_kh from hocnq_ttkd.temp_plkh where khachhang_id=a.khachhang_id)
												end)
				    -- select loaitb_id, dich_vu, ma_tb, ngay_bbbg, nguon, thuebao_id from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202404 and chuquan_id in (145,264,266) and loaitb_id<>21 and phanloai_kh is null
				    ;
 
                    
				update ttkd_bsc.ct_bsc_ptm set heso_khachhang=''
				    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') and heso_khachhang is not null
				    ;
                   
				update ttkd_bsc.ct_bsc_ptm a
				    set heso_khachhang = case when heso_kvdacthu<1 then 1                                                       -- ap dung cho thue bao thuoc kv doc quyen, Mai linh, Coop, vnpts Hoa Binh 
														   else case when phanloai_kh in ('DA1','DA2','DB1') then 0.7
																		 when phanloai_kh in ('DB2') then 0.85
																		 when phanloai_kh in ('DB3','DC1','DC2') then 1
																		 else 1 end end
				-- select heso_khachhang from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') and heso_khachhang is null
				    ;

          commit;
/*
select heso_khachhang , count(*)
    from ct_bsc_ptm
    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') 
    group by heso_khachhang;   
*/
 
        
-- He so thue bao ngan han: 
			update ttkd_bsc.ct_bsc_ptm a set heso_tbnganhan=0.3
			-- 	select heso_tbnganhan from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202404 and thoihan_id=1 and dichvuvt_id in (1,10,11,4,7,8,9)
			    ;
                           commit;
                       
-- He so dich vu DNHM+sodep:
				update ttkd_bsc.ct_bsc_ptm a set heso_dichvu_dnhm=null
				    where thang_ptm=202404 and loaitb_id<>21 and ma_kh<>'GTGT rieng'
					   and (tien_dnhm>0 or tien_sodep>0)
					   ;   
        
                                    
				update ttkd_bsc.ct_bsc_ptm a 
					   set heso_dichvu_dnhm = (case when loaitb_id in (280, 292) then 0.3  -- phi tich hop
																   when loaitb_id=35 then 0.4       -- VNPT iOffice-phi tich hop nhap o tien dnhm
																   else 0.1 end)
				    -- select heso_dichvu_dnhm from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng')
				    ;
                          commit;          
        
-- he so ho tro:
				update ttkd_bsc.ct_bsc_ptm set heso_hotro_nvptm='' 
				    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') and heso_hotro_nvptm is not null
				    ;
												    
				update ttkd_bsc.ct_bsc_ptm a
				    set heso_hotro_nvptm  = case when manv_hotro is not null and tyle_am is not null then tyle_am 
															   when manv_tt_dai is not null and manv_tt_dai<>manv_ptm then 0.5
															   when manv_ptm='VNP017772' and loaitb_id=288 
																		 and (hdtb_id, thuebao_id) in (select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL where thang = a.thang_ptm)
																					then 0.5   -- SmartCA tinh cho Huong BHOL 50%
															  else 1 end
						,heso_hotro_nvhotro = case when manv_hotro is not null then tyle_hotro else null end
						,heso_hotro_dai         = case when manv_tt_dai is not null and manv_tt_dai<>manv_ptm then 0.5 end
		-- select heso_hotro_nvptm, heso_hotro_nvhotro, heso_hotro_dai from ttkd_bsc.ct_bsc_ptm a    
				where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') 
							-- and (hdtb_id, thuebao_id) in ( select hdtb_id, thuebao_id from hocnq_ttkd.ptm_xuly_khieunai_BHOL_202404 )
							--and (hdtb_id, thuebao_id) in ( select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL )
							
				    ; 
                         commit;  
					rollback;

/* 
select dich_vu, ma_tb, loaitb_id, manv_hotro, tyle_hotro, heso_hotro_nvptm, heso_hotro_nvhotro from ct_bsc_ptm 
    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') and heso_hotro_nvptm is null ;
    
select dich_vu, ma_tb, loaitb_id, manv_hotro, tyle_hotro, heso_hotro_nvptm, heso_hotro_nvhotro from ct_bsc_ptm 
    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng')
        and manv_hotro is not null and heso_hotro_nvhotro is null;

select ma_tb, ten_pb, heso_hotro_dai from ct_bsc_ptm 
    where thang_ptm=202404 and loaitb_id<>21 and ma_kh<>'GTGT rieng' 
        and manv_tt_dai is not null and (heso_hotro_dai<>0.5 or heso_hotro_dai is null);
*/


-- He so dia ban tinh khac: 
			update ttkd_bsc.ct_bsc_ptm 
				   set heso_diaban_tinhkhac = 0.85       
	---		select heso_diaban_tinhkhac from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202404 and loaitb_id not in (20,21,149) and heso_diaban_tinhkhac is null
				   and ( (dichvuvt_id in (1,10,11,12,4,7,8,9) and kieuld_id=13102)
						 or (dichvuvt_id in (13,14,15,16) and tinh_id not in (28, 8,36))) 
				;
           
-- Luu y kiem tra heso_diaban_tinhkhac cho GTGT rieng

        
/*-- Kiem tra:
select dich_vu, ma_tb, heso_diaban_tinhkhac, diachi_ld, tinh_id, ttvt_id from ct_bsc_ptm
    where thang_ptm=202404 and (loaitb_id not in (20,21,149) or ma_kh='GTGT rieng' ) 
        and heso_diaban_tinhkhac is not null;             
*/
 

-- Kiem tra thue bao tinh doanh thu nhieu lan trong thang:
select ma_gd, ma_tb, nguon, loaihd_id, dthu_goi, kieuld_id,lydo_khongtinh_luong, thang_tldg_dt, lydo_khongtinh_dongia
    from ttkd_bsc.ct_bsc_ptm
    where thang_ptm=202404 and loaitb_id<>21 and
                (ma_gd, ma_tb, loaitb_id) in 
                          (select ma_gd, ma_tb, loaitb_id from ttkd_bsc.ct_bsc_ptm 
                            where thang_ptm=202404 and loaitb_id!=20 group by ma_gd, ma_tb, loaitb_id having count(*)>1);
                            
-- Vua tai lap dich vu (loaihd_id=7, ngung su dung qua 35 ngay) vua nang/ha cap : xet doanh thu tai lap dich vu theo goi moi (loaihd_id=7)
update ttkd_bsc.ct_bsc_ptm a set lydo_khongtinh_luong=lydo_khongtinh_luong||'-Tinh luong theo hs tai lap'
    where thang_ptm=202404 and nguon like 'thaydoitocdo%' and lydo_khongtinh_luong not like '%Tinh luong theo hs tai lap%'
        and exists (select 1 from ttkd_bsc.ct_bsc_ptm
                            where thang_ptm=202404 and thuebao_id=a.thuebao_id and nguon like 'tailap%')
					   ;
		
		commit;
/*
-- Vua lap moi vua ccq: xet doanh thu lap moi theo he so qui dinh
update ct_bsc_ptm a set lydo_khongtinh_luong=lydo_khongtinh_luong||'-Tinh luong theo hs lap moi'
    where thang_ptm=202403 and nguon like 'ccq%'
            and exists (select 1 from ct_bsc_ptm
                                where thang_ptm=202403 and thuebao_id=a.thuebao_id and nguon like 'ptm_%');


-- Vua nang/ha cap, vua chuyen chu quyen (kieu lap moi):
update ct_bsc_ptm a 
    set  lydo_khongtinh_luong=lydo_khongtinh_luong||'-Tinh luong theo hs ccq',
            lydo_khongtinh_dongia='-Tinh luong theo hs ccq',
            thang_tldg_dt='', thang_tlkpi='', thang_tlkpi_to='', thang_tlkpi_phong=''
    where thang_ptm=202403 and  loaihd_id=8  
        and exists (select 1 from ct_bsc_ptm
                            where thang_ptm=202403 and thuebao_id=a.thuebao_id and loaihd_id=2);


-- Vua tai lap dich vu (ngung su dung qua 35 ngay) vua chuyen chu quyen (kieu lap moi):
update ct_bsc_ptm a set lydo_khongtinh_luong=lydo_khongtinh_luong||'-Tinh luong theo hs tai lap'
    where thang_ptm=202403 and  loaihd_id=2 
                         and exists (select 1 from ct_bsc_ptm
                                                where thang_ptm=202403 and thuebao_id=a.thuebao_id and loaihd_id=7);

*/

--=====

-- DTHU DON GIA 
			update ttkd_bsc.ct_bsc_ptm a 
						set dongia = 858
		---	    select dongia from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng' ) and dongia is null 
			    ;                            
           
           
-- cac dv ngoai tru vnptt, SMS Brandname, Voice Brandnanme
			update ttkd_bsc.ct_bsc_ptm a 
			    set doanhthu_dongia_nvptm   = round(dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
															*heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
															*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
															*nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)*nvl(heso_daily,1) ,0)                                                                            
					,doanhthu_dongia_nvhotro = round(dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
															*heso_quydinh_nvhotro*nvl(heso_kvdacthu,1) * nvl(heso_vtcv_nvhotro, 1)
															*tyle_hotro*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
															*nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
					,doanhthu_dongia_dai          = round(dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
															*heso_quydinh_dai*heso_vtcv_dai*heso_hotro_dai*nvl(heso_khachhang,1)
															*nvl(heso_tbnganhan,1)*nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
	---	    select doanhthu_dongia_nvptm, doanhthu_dongia_nvhotro, doanhthu_dongia_dai from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202404 and (loaitb_id not in (21,131,358) or ma_kh='GTGT rieng') 
			    ;                       

            commit;
-- Doanh thu goi tich hop: ap dung khong phan biet tap quan ly 
        -- tham khao bang huong dan cua anh Nghia tinh he so cac goi tich hop cho SMEs: VB 275/TTKD HCM-DH 22/06/2020:
				update ttkd_bsc.ct_bsc_ptm a
				    set doanhthu_dongia_nvptm =
								    (case when goi_id=15599  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ) * 0.21/0.1434 ,0)  -- SME_NEW
											 when goi_id=15600  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ) * 0.25/0.17 ,0)  -- SME+
											 when goi_id=15602  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ) * 0.25/0.21 ,0)  -- SME_BASIC 1
											 when goi_id=15601  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ) * 0.35/0.30 ,0)  -- SME_BASIC 2   
											 when goi_id=15604  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ) * 0.19/0.13 ,0)  -- SME_SMART1
											 when goi_id=15603  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ) * 0.20/0.14 ,0)  -- SME_SMART2
											 when goi_id=15605  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ) * 0.20/0.16 ,0)  -- F_Pharmacy
											 when goi_id=15596  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)  ) * 0.20/0.15 ,0)  -- F_ORM
									end)   
	---	    select doanhthu_dongia_nvptm from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202404 and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605) 
				    ;
                      
                            
-- Luong don gia cac dv ngoai tru vnptt, SMS Brandname:
				update ttkd_bsc.ct_bsc_ptm a 
				    set luong_dongia_nvptm    = round(nvl(doanhthu_dongia_nvptm,0)*dongia/1000 ,0)
						,luong_dongia_nvhotro = round(nvl(doanhthu_dongia_nvhotro,0)*dongia/1000 ,0)
						,luong_dongia_dai         = round(nvl(doanhthu_dongia_dai,0)*dongia/1000 ,0)
		--- select luong_dongia_nvptm, luong_dongia_nvhotro, luong_dongia_dai from ttkd_bsc.ct_bsc_ptm a 
				    where thang_ptm=202404 and (loaitb_id not in (21,131,358) or ma_kh='GTGT rieng') 
				    ;
                            
       commit;
-- SMS Brandname thang n-1: 
			update ttkd_bsc.ct_bsc_ptm a 
			    set doanhthu_dongia_nvptm = round(((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1))
															*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm
															*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)
															*heso_khachhang*nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1)*nvl(heso_daily,1) ,0)
					,doanhthu_dongia_nvhotro = round(((nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
																		 *nvl(heso_quydinh_nvhotro,1)*nvl(heso_vtcv_nvhotro,1)*heso_hotro_nvhotro*nvl(heso_khachhang,1) ,0)
					,doanhthu_dongia_dai =null
---	   select * from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm='202403' and loaitb_id=131 
			    ;
									    
			update ttkd_bsc.ct_bsc_ptm 
			    set luong_dongia_nvptm = nvl(doanhthu_dongia_nvptm,0)*dongia/1000
			    where thang_ptm='202403' and loaitb_id=131
			    ;
                           
commit;
            
-- don gia dnhm: 

			update ttkd_bsc.ct_bsc_ptm a 
			    set doanhthu_dongia_dnhm = round((nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *heso_dichvu_dnhm
														 * heso_quydinh_nvptm * nvl(heso_kvdacthu,1) * heso_vtcv_nvptm
														 * nvl(heso_diaban_tinhkhac,1) * nvl(tyle_huongdt,1) * nvl(heso_daily,1) ,0)
		--select doanhthu_dongia_dnhm from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202404 
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
							   and thang_ptm = 202404 and (loaitb_id not in (20,21) or ma_kh='GTGT rieng' or (loaitb_id=20 and goi_luongtinh is null)) 
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
				    where thang_ptm=202404 and dich_vu not like 'Thi_t b_ gi_i ph_p%' and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') 
				    ;


-- dtkpi cua dich vu Thiet bi giai phap:  la dthu hop dong x cac he so tinh bsc (ko tinh tren chenh lech thu chi).
					update ttkd_bsc.ct_bsc_ptm a
						   set doanhthu_kpi_nvptm  = round(dthu_goi_goc * nvl(heso_dichvu,1) * nvl(heso_quydinh_nvptm,1)
																 * decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) * nvl(heso_tbnganhan,1)
																 * nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) * nvl(heso_daily,1) ,0)  
							   ,doanhthu_kpi_nvhotro = case when manv_hotro is not null 
																		   then round(dthu_goi_goc * nvl(heso_dichvu,1) * nvl(heso_quydinh_nvhotro,1) * tyle_hotro 
																				 * nvl(heso_tbnganhan,1) * nvl(heso_diaban_tinhkhac,1) * nvl(tyle_huongdt,1) ,0) else null end         
			---	select doanhthu_kpi_nvhotro from 	ttkd_bsc.ct_bsc_ptm a
					    where thang_ptm=202404 and dich_vu like 'Thi_t b_ gi_i ph_p%'  
					    ; 
           
                            
            
-- SMS Brandname thang n-1:
			update ttkd_bsc.ct_bsc_ptm
			    set  doanhthu_kpi_nvptm = doanhthu_dongia_nvptm,
					  doanhthu_kpi_nvhotro = doanhthu_dongia_nvhotro
			    where thang_ptm='202403' and loaitb_id=131
			    ;
                            
            
-- DTHU KPI PHONG 
				update ttkd_bsc.ct_bsc_ptm a
					   set doanhthu_kpi_phong = (case when dich_vu like 'Thi_t b_ gi_i ph_p%' 
																		  then round(dthu_goi_goc * nvl(heso_dichvu,1) * nvl(heso_khuyenkhich,1) * nvl(heso_tratruoc,1) * nvl(heso_kvdacthu,1)         
																						    * decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) * nvl(heso_khachhang,1) 
																						    * nvl(heso_tbnganhan,1) *nvl(heso_diaban_tinhkhac,1) * nvl(tyle_huongdt,1) ,0)
																	  else round(dthu_goi * nvl(heso_dichvu,1) *nvl(heso_khuyenkhich,1) * nvl(heso_tratruoc,1) * nvl(heso_kvdacthu,1)         
																						    * decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) * nvl(heso_khachhang,1) 
																						    * nvl(heso_tbnganhan,1) *nvl(heso_diaban_tinhkhac,1) * nvl(tyle_huongdt,1) ,0) end)
		---   select doanhthu_kpi_phong from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202404 and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') 
				    ;
                      commit;
    
    
-- kpi goi SME: ko xet heso_quydinh_nvptm vi ko phan biet tap KH
				update ttkd_bsc.ct_bsc_ptm a
				    set doanhthu_kpi_phong=round(dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
														  *nvl(heso_kvdacthu,1) *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
														  *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
						  ,doanhthu_dongia_nvhotro =null 
						  ,doanhthu_dongia_dai =round(dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_dai*heso_vtcv_dai*heso_hotro_dai*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
				    where thang_ptm=202404 and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605)
				    ;

 --  SMS Brandname thang n-1
				update ttkd_bsc.ct_bsc_ptm
				    set doanhthu_kpi_phong= round(((nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
														    *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
														    *decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) 
														    *nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
														    *nvl(heso_diaban_tinhkhac,1) *nvl(tyle_huongdt,1) ,0)
				    -- select dthu_goi, dthu_goi_ngoaimang, doanhthu_kpi_nvptm from ct_bsc_ptm
				    where thang_ptm='202403' and loaitb_id=131
				    ;
                            
            commit;
-- Kpi dnhm phong:
		update ttkd_bsc.ct_bsc_ptm a
		    set doanhthu_kpi_dnhm_phong = round( (nvl(tien_dnhm,0)+nvl(tien_sodep,0))*nvl(heso_dichvu_dnhm,1)
														    * nvl(heso_diaban_tinhkhac,1) * nvl(tyle_huongdt,1),0)
	---	select doanhthu_kpi_dnhm_phong from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm=202404 and (loaitb_id not in (20,21) or ma_kh='GTGT rieng' or (loaitb_id=20 and goi_luongtinh is null)) 
					   and (tien_dnhm>0 or tien_sodep>0)
					   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
					   ;
                                 


--- DTHU KPI TO:
				update ttkd_bsc.ct_bsc_ptm a
				    set doanhthu_kpi_to= doanhthu_kpi_phong
	---	select doanhthu_kpi_to from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202404 and (loaitb_id!=21 or ma_kh='GTGT rieng')  
				    ;
                    
               
-- AM ban qua dai ly:
				update ttkd_bsc.ct_bsc_ptm a
				    set doanhthu_kpi_nvptm = (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * dthu_goi * nvl(heso_tratruoc,1)
															 else heso_daily * dthu_goi end)
					    ,doanhthu_dongia_nvptm = (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * dthu_goi * nvl(heso_tratruoc,1)
															 else heso_daily * dthu_goi end)
					    ,luong_dongia_nvptm = (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * dthu_goi * nvl(heso_tratruoc,1) * 0.858
															 else heso_daily * dthu_goi * 0.858 end)
						, doanhthu_kpi_dnhm = (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * (nvl(tien_dnhm,0)+nvl(tien_sodep,0))
															 else heso_daily * (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) end)
					    ,doanhthu_dongia_dnhm = (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * (nvl(tien_dnhm,0)+nvl(tien_sodep,0))
															 else heso_daily * (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) end)
					    ,luong_dongia_dnhm_nvptm = (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) * 0.858
															 else heso_daily * (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) * 0.858 end)         
		/*		     select ma_tb, manv_ptm, ma_vtcv, ma_nguoigt, heso_tratruoc, dthu_goi, heso_daily
									, doanhthu_dongia_nvptm,luong_dongia_nvptm, doanhthu_kpi_nvptm 
									,doanhthu_dongia_dnhm,luong_dongia_dnhm_nvptm, doanhthu_kpi_dnhm
							    , (case when exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
																	   then heso_daily * dthu_goi * nvl(heso_tratruoc,1) * 0.858
															 else heso_daily * dthu_goi * 0.858 end)  newa
									from ttkd_bsc.ct_bsc_ptm a
					
				    where  thang_ptm=202401 and (loaitb_id!=21 or ma_kh='GTGT rieng') 
							 and heso_daily=0.05 
				;
                
		commit;
-- ======

-- Kiem tra:
-- Cac thue bao chua co doanhthu_dongia_nvptm:
select thang_ptm, chuquan_id,manv_ptm, lydo_khongtinh_luong,nguon,dich_vu, dichvuvt_id,loaitb_id, hdtb_id, thuebao_id,doituong_id,
            ma_gd, ma_tb,loaitb_id, ngay_bbbg,thoihan_id,
            manv_ptm, tennv_ptm, ten_pb, 
            goi_id,dthu_goi_goc,dthu_goi,dthu_ps,dthu_ps_n1,heso_dichvu,heso_khuyenkhich,heso_tratruoc,heso_quydinh_nvptm,heso_vtcv_nvptm,heso_hotro_nvptm  ,
            doanhthu_dongia_nvptm, luong_dongia_nvptm, trangthai_tt_id
from ttkd_bsc.ct_bsc_ptm a
where thang_ptm=202404
            and loaitb_id not in (21,210) and ma_kh<>'GTGT rieng' 
            and lydo_khongtinh_luong is null and manv_ptm is not null and dthu_ps is not null
            and doanhthu_dongia_nvptm is null
            and loaitb_id not in (20,61,131,222,224,358) ; -- mytv home combo, sms brn


-- kiem tra doanhthu_dongia_nvptm<>doanhthu_kpi_nvptm:
select thang_ptm, ma_tb, manv_ptm,ma_vtcv, dthu_goi, heso_dichvu, heso_quydinh_nvptm, heso_vtcv_nvptm, 
        heso_khuyenkhich, heso_tratruoc, doanhthu_kpi_nvptm dthu_kpi_nvptm, doanhthu_dongia_nvptm dthu_dongia_nvptm, 
        dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1) dthu_dongia_new 
from ttkd_bsc.ct_bsc_ptm
where thang_ptm=202404 
            and loaitb_id not in (21,210) and ma_kh<>'GTGT rieng' 
            and lydo_khongtinh_luong is null
            and doanhthu_dongia_nvptm<>doanhthu_kpi_nvptm;


-- nv ho tro chua duoc tinh doanhthu_kpi_nvhotro:
select thang_luong, thang_ptm,dich_vu, dichvuvt_id, loaitb_id, tien_dnhm, tocdo_id, goi_id, dthu_ps, thuebao_id,ma_tb,dthu_goi,doanhthu_kpi_nvhotro , chuquan_id, heso_quydinh_nvhotro
    from ttkd_bsc.ct_bsc_ptm
    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng') 
    and manv_hotro is not null and dthu_ps is not null and doanhthu_kpi_nvhotro is null
    ;

-- dnhm:
select dichvuvt_id, loaitb_id, ma_tb, tien_dnhm, tien_sodep, (nvl(tien_dnhm,0)+nvl(tien_sodep,0)),heso_dichvu_dnhm, heso_quydinh_nvptm, nvl(heso_kvdacthu,1), heso_vtcv_nvptm, 
            doanhthu_dongia_dnhm,  luong_dongia_dnhm_nvptm, doanhthu_kpi_dnhm
from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm=202404 and (loaitb_id!=21 or ma_kh<>'GTGT rieng')
                                  and (tien_dnhm>0 or tien_sodep>0)
                                  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id );
             
           
-- SMS Brn:
select ma_tb, dthu_ps, dthu_ps_n1, dthu_goi, dthu_goi_ngoaimang, heso_dichvu, heso_dichvu_1,
            doanhthu_dongia_nvptm, doanhthu_kpi_nvptm, luong_dongia_nvptm, doanhthu_kpi_nvhotro, thang_tlkpi_hotro
    from ct_bsc_ptm
    where thang_ptm='202403' and loaitb_id=131;

-- TB ngan han:
select thang_ptm, ma_pb,manv_ptm,(select ten_nv from ttkd_bsc.nhanvien_202404 where manv_hrm=a.manv_ptm) ten_nv,
            ma_tb, ten_tb,dich_vu, ngay_bbbg,ngay_cat, trangthaitb_id,songay_sd, heso_tbnganhan,tien_dnhm,tien_tt, ngay_tt, soseri, dthu_goi, dthu_ps,
            doanhthu_dongia_nvptm, luong_dongia_nvptm,  doanhthu_kpi_nvptm, 
            luong_dongia_dnhm_nvptm, thang_tldg_dnhm
    from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm=202404 and thoihan_id=1
            and dthu_goi<>dthu_ps;
   




