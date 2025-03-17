/*Mail: Xác định số liệu thù lao PTM của nhân viên kỹ thuật tháng 12/2024 để đưa lên biểu B11 đối soát
Ngay 3/2/2025

K/g anh Sơn - TP NS VTTP,
      em Dự - PP KTKH VTTP,

Hiện tại, PKTNV áp dụng vb 353/ TTr- NS- ĐH, các thuê bao VTTP PTM thỏa điều kiện mới được ghi nhận, chi trả nên sẽ có một số thuê bao PTM của 3 tháng cuối năm, chi trả trong năm 2025 khi đủ điều kiện.

Để đáp ứng quyết toán như PKTKH VTTP yêu cầu, PKTNV sẽ xử lý & gởi VTTP số liệu TB PTM trong 3 tháng cuối năm đã nghiệm thu, không ràng điều kiện KH đã thanh toán & hồ sơ đã lưu kho.

 @Học: em xử lý & gởi VTTP trên dòng mail này nhé. 
*/
--thang_luong in (86, 87, 70) --> file chuong trinh ngoai A Nghia --> thang_ptm = 202407 --> se chay
								
--thang_luong = 5 --> duplicate Record để tính them khi duỵet
--thang_luong = 4 --> dung thu chuyen dung that file 5, file 6 update thong tin duan de tinh heso & thay doi cac heso, file 5 update DIGISHOP, file 6 update MANV_PTM, VTCV

--thang_luong = 3 --> he so goc nop tre file 6d, file 6d tinh phan chia dthu, tinh lai dthu
--thang_luong = 2 --> bo sung tra truoc file 6
--thang_luong = 1 --> cap nhat tbao ngan han file 6, tinh lại các loại dthu dongia và kpi

--file nay chay 2 dot, full code
		/*Danh sach cac he so
		heso_dichvu, heso_dichvu_1, phanloai_kh, heso_khachhang, heso_tbnganhan, heso_tratruoc
		, heso_khuyenkhich, heso_kvdacthu, heso_vtcv_nvptm, heso_vtcv_dai, heso_vtcv_nvhotro
		, heso_hotro_nvptm, heso_hotro_dai, heso_hotro_nvhotro, heso_quydinh_nvptm, heso_quydinh_dai
		, heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_hoso, heso_dichvu_dnhm, dongia
		*/
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
        
	   	--khi can thiet chay--BSUNG ma nhan vien tiep thi
			UPDATE ttkd_bsc.ct_bsc_ptm a
										set manv_ptm = (select y.ma_nv 
																	from css_hcm.hd_khachhang x
																						join admin_hcm.nhanvien_onebss y on x.ctv_id = y.nhanvien_id
																		where x.hdkh_id = a.hdkh_id)
												, thang_luong = 4
--					select thang_luong, thang_ptm, thang_tldg_dt, THANG_TLKPI_PHONG, thuebao_id, manv_ptm from ttkd_bsc.ct_bsc_ptm a
								where manv_ptm is null
											and exists (select 1 from css_hcm.hd_khachhang where ctv_id is not null and hdkh_id = a.hdkh_id
																		and exists (select 1 from ttkd_bsc.nhanvien where ctv_id = nhanvien_id))
											and thang_ptm >= 202412
											and thang_tldg_dt is null			--- chua tinh luong
							;
							
			-----CODE UPDATE NV MOI
				update ttkd_bsc.ct_bsc_ptm a 
							    set (tennv_ptm, ma_to, ten_to, ma_pb, ten_pb, ma_vtcv, ten_vtcv,  loai_ld, nhom_tiepthi)
									= (select TEN_NV, MA_TO, TEN_TO, MA_PB, TEN_PB, MA_VTCV, ten_vtcv, LOAI_LD, NHOMLD_ID
											from 
													(select ma_nv, ten_nv, ma_to, ten_to, ma_pb, ten_pb, ma_vtcv, ten_vtcv, loai_ld, nhomld_id
																, thang, row_number() over (partition by ma_nv order by thang desc) rnk
													  from ttkd_bsc.nhanvien 
													  where thang = 202502
																	
													  ) 
											where ma_nv = a.manv_ptm and thang = a.thang_ptm
											) 
--			select  * from ttkd_bsc.ct_bsc_ptm a
							    where  a.thang_ptm >= 202412 and id = 10874131
						;
				----END
	   
-- Tien thanh toan hop dong dau vao:  (xet ngay thanh toan het ngay 7)
		select * from hocnq_ttkd.temp_tt
					where ma_gd in ('HCM-LD/01600473')
				;
		commit;
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
				    where kh.hdkh_id = tb.hdkh_id and tb.hdtb_id = b.hdtb_id and a.phieutt_id = b.phieutt_id and a.trangthai in (1,2) --and b.tien >0 
										and tb.tthd_id = 6
								and to_number(to_char(a.ngay_cn,'yyyymm')) >= 202411					--thang n -3
--								and tb.hdtb_id in (26277035, 26276940)
--								and a.hdkh_id not in (select hdkh_id from ttkd_hcm.temp_tt)
--								and kh.ma_gd in ('00978797', '00969839', '00969855', '00969842')
				group by kh.loaihd_id, a.ma_gd, a.hdkh_id, b.hdtb_id,a.ngay_cn, a.ngay_tt, a.soseri, a.trangthai--, a.ht_tra_id
						;
		---move TTKDDB				
			drop table hocnq_ttkd.temp_tt purge;
			create table hocnq_ttkd.temp_tt as
--			insert into hocnq_ttkd.temp_tt
					select * from ttkd_hcm.temp_tt@dataguard a 
								where trunc(nvl(ngay_tt, ngay_cn)) < '08/03/2025'			---thang n+1
											--and ma_gd in ('00978797', '00969839', '00969855', '00969842')
--											and not exists (select 1 from hocnq_ttkd.temp_tt where hdtb_id =  a.hdtb_id)
											;
								where ma_gd in (
'00906613', 'HCM-LD/01826296', 'HCM-LD/01922724', 'HCM-LD/01924795', 'HCM-LD/01946848', 'HCM-LD/01938255', 'HCM-LD/01947517', 'HCM-LD/01948772')
			;
			create index hocnq_ttkd.temp_tt_hdtbid on hocnq_ttkd.temp_tt (hdtb_id)
			;
			select * from hocnq_ttkd.temp_tt; where hdtb_id in (26277035, 26276940);
		
		
			MERGE INTO ttkd_bsc.ct_bsc_ptm a
			USING (select ngay_tt, soseri, tien, trangthai, hdtb_id, ma_gd--, case when tien > 0 then ht_tra_id else null end ht_tra_id 
							from hocnq_ttkd.temp_tt) b
			ON (b.hdtb_id = a.hdtb_id)
			WHEN MATCHED THEN
					update SET ngay_tt = b.ngay_tt, soseri = b.soseri , tien_tt = b.tien, trangthai_tt_id = 1--, ht_tra_id = b.ht_tra_id and 
--									, thang_luong = 4
---			select ma_tb, thuebao_id, ngay_tt, soseri, tien_tt, trangthai_tt_id, hdtb_id, ma_gd from ttkd_bsc.ct_bsc_ptm a
			WHERE thang_ptm >= 202411 and chuquan_id <> 264 and thang_tldg_dt is null and nvl(trangthai_tt_id,0) = 0			--thang n -3
								and loaitb_id not in (20, 21) --and ma_tb = 'hcm_hddt_00010808'
								and exists(select 1 from hocnq_ttkd.temp_tt where hdtb_id = a.hdtb_id) 
--								and ma_gd in ('HCM-LD/01939638')
								;
			---*** Doi toc do Fiber không xet trang thai thu tien nếu không có dat cọc**--
			update  ttkd_bsc.ct_bsc_ptm a set trangthai_tt_id = 1--, vanban_id = 89
---			select thang_ptm, ma_gd, ma_tb, thuebao_id, ngay_tt, soseri, tien_tt, trangthai_tt_id, hdtb_id, datcoc_csd, tenkieu_ld, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			where thang_ptm >= 202411 and chuquan_id <> 264 and thang_tldg_dt is null and nvl(trangthai_tt_id,0) = 0			--thang n -3
								and dichvuvt_id in (4,7,8,9) and kieuld_id in (24, 596) and datcoc_csd is null
							
			;
			
commit;
rollback;
                                                                     
-- DTHU_PS:
-- Dot 1: da update ps 1 lan 4/2/25
		drop table ttkd_bsc.tmp_ct_no purge
		;
--	select * from ttkd_bsc.tmp_ct_no;

		create table ttkd_bsc.tmp_ct_no as 
			select * from ttkd_hcm.tmp_ct_no@dataguard 
		;
				select thuebao_id, khoanmuctt_id, nogoc 
						from bcss.v_ct_no@dataguard 
						where phanvung_id=28 and ky_cuoc=20250201
					;
		create index ttkd_bsc.idx_ctno_tbid on ttkd_bsc.tmp_ct_no (thuebao_id);
   
-- dthu_ps thang n:
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps = (select sum(nogoc) from ttkd_bsc.tmp_ct_no
								    where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
		    where thang_ptm = 202502 and loaitb_id<>21 and dthu_ps is null 
			   and exists (select 1 from ttkd_bsc.tmp_ct_no
								where nogoc>0 and khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
		;
     
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps_n1 = (select sum(nogoc) from ttkd_bsc.tmp_ct_no
										 where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
		    where thang_ptm = 202501 and loaitb_id not in (20,21,131)
			   and dthu_ps_n1 is null
			   and exists (select 1 from ttkd_bsc.tmp_ct_no
							   where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
					   ;
            
			update ttkd_bsc.ct_bsc_ptm a 
				   set dthu_ps_n2 = (select sum(nogoc) from ttkd_bsc.tmp_ct_no
											 where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
			    where thang_ptm = 202412 and loaitb_id not in (20,21,131)
				   and dthu_ps_n2 is null
				   and exists (select 1 from ttkd_bsc.tmp_ct_no
				   where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
			;

                            
			update ttkd_bsc.ct_bsc_ptm a 
				   set dthu_ps_n3 = (select sum(nogoc) from ttkd_bsc.tmp_ct_no
											 where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id)
			    where thang_ptm = 202411 and loaitb_id not in (20,21,131)
				   and dthu_ps_n3 is null
				   and exists (select 1 from ttkd_bsc.tmp_ct_no
								   where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) and thuebao_id=a.thuebao_id);          
commit;
                       
-- Dot 2: vi co cuoc VNP, CNTT, GTGT vi sau ngay 5 moi tinh cuoc xong
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps = (select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd where ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
--		     select * from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202502 and loaitb_id<>21 and nvl(dthu_ps, 0) = 0 
			   and exists(select 1 from ttkd_bct.cuoc_thuebao_ttkd where dthu>0 and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
             ;   
                            
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps_n1 = (select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd where ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
--		    select * from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202501 and loaitb_id<>21 and nvl(dthu_ps_n1, 0) = 0
			   and exists(select 1 from ttkd_bct.cuoc_thuebao_ttkd where dthu>0 and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
			   ;
                
		update ttkd_bsc.ct_bsc_ptm a 
			   set dthu_ps_n2 = (select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd where ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
--		    select * from ttkd_bsc.ct_bsc_ptm a		    
		    where thang_ptm = 202412 and loaitb_id<>21 and nvl(dthu_ps_n2, 0) = 0
			   and exists(select 1 from ttkd_bct.cuoc_thuebao_ttkd where dthu>0 and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
			   ;
                            
			update ttkd_bsc.ct_bsc_ptm a 
				   set dthu_ps_n3 = (select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd where ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
--		    select * from ttkd_bsc.ct_bsc_ptm a		    
			    where thang_ptm = 202411 and loaitb_id<>21 and nvl(dthu_ps_n3, 0) = 0
				   and exists(select 1 from ttkd_bct.cuoc_thuebao_ttkd where dthu>0 and ma_tb=a.ma_tb and loaitb_id=a.loaitb_id)
				   ;
commit;

-- DTHU GOI TB NGAN HAN THANG n:
		-- Ap dung Fiber, Internet trưc tiep, TSL, Colocation
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
--		    select thang_luong, thang_ptm, ma_tb, trangthaitb_id, ngay_bbbg, ngay_cat, (select ngay_cat from css_hcm.db_thuebao where thuebao_id=a.thuebao_id) ngay_cat_dbonline, dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3, dthu_goi, nvl(a.dthu_ps,0)+nvl(a.dthu_ps_n1,0)+nvl(a.dthu_ps_n2,0)+nvl(a.dthu_ps_n3,0) dthu_goi_new, ghi_chu from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm >= 202411 and thoihan_id=1 and (loaitb_id in (58, 59, 39) or dichvuvt_id in (7, 8, 9)) ---thang n-3
					   and (thang_tldg_dt is null 
									or thang_tldg_dt = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm')) 	---thang n
								)										
--					   and nvl(a.dthu_ps,0)+nvl(a.dthu_ps_n1,0)+nvl(a.dthu_ps_n2,0)+nvl(a.dthu_ps_n3,0) > nvl(dthu_goi,0)
					   and exists(select 1 from css_hcm.db_thuebao 
												where trangthaitb_id in (7, 9) and to_number(to_char(ngay_cat,'yyyymm')) = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm')) --thang n
															and thuebao_id = a.thuebao_id
										)  
			   ;      
            commit;
  
-- Tinh lai dthu goi cho CA, IVAN, HDDT, TAX, VNPT HKD chua co dthu_goi tai thang_ptm, doi voi cac hop dong GIA HAN:
			----Tbao DTHU_goi = 0, nhung tong dthu_ps >0 --> UPDATE DATCOC_CSD into DTHU_GOI
			----Ap dung dich vu thang tinh 1 lan
					
					drop table ttkd_bsc.tmp_db_datcoc purge
							;
							create table ttkd_bsc.tmp_db_datcoc as 
									select thuebao_id, cuoc_dc, thang_kt, thang_kt_dc, thang_huy, thang_bd
											from css.v_db_datcoc@dataguard 
											where CUOC_DC > 0 and hieuluc=1 and ttdc_id = 0 and to_number(to_char(ngay_cn,'yyyymm')) >= 202411
														and thang_bd >= 202411
										;
							create index ttkd_bsc.idx_dc_tbid on ttkd_bsc.tmp_db_datcoc (thuebao_id)
							;
				drop table hocnq_ttkd.temp_ps purge
				;
			
				create table hocnq_ttkd.temp_ps as
--insert into hocnq_ttkd.temp_ps
						with 
--								   ps as (select thuebao_id, sum(nogoc) dthu_ps_n
--										 from ttkd_bsc.tmp_ct_no
--											where khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953)
--											group by thuebao_id
--										 )
								    datcoc as (select thuebao_id, sum(round(cuoc_dc/1.1,0)) datcoc_csd_new
																, min(months_between( to_date(least(thang_kt,nvl(thang_kt_dc,'999999'),nvl(thang_huy,'999999')) , 'yyyymm') , 
																																	    to_date(thang_bd,'yyyymm'))+ 1) as sothang_dc_new
																, min(thang_kt) thang_ktdc_new, min(thang_bd) thang_bddc_new
--										select *
										 from ttkd_bsc.tmp_db_datcoc
										 --where hieuluc=1 and ttdc_id = 0 and to_number(to_char(ngay_cn,'yyyymm')) >= 202410
										 group by thuebao_id
										 )
							select thang_ptm, a.thuebao_id, ma_gd, ma_tb, dthu_goi, dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3
										  , nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0) dthu_tong, datcoc_csd, sothang_dc, thang_bddc
										  , datcoc_csd_new, sothang_dc_new, thang_ktdc_new, thang_bddc_new
										 ,thang_tldg_dt, thang_tlkpi_to, thang_tlkpi_phong, lydo_khongtinh_dongia
								from ttkd_bsc.ct_bsc_ptm a
										    join datcoc on a.thuebao_id = datcoc.thuebao_id
								where thang_ptm >= 202411 and nvl(dthu_goi, 0) = 0 --dthu_goi is null		---thang n -3
										  and (thang_tldg_dt is null 
																or thang_tldg_dt = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))											---thang n
													)
										  and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302)
										  and exists(select 1 from ttkd_bsc.tmp_db_datcoc where thuebao_id = a.thuebao_id)
--										  and ma_tb = 'hcm_ca_00067906'
				;
				commit;
      
      
			update ttkd_bsc.ct_bsc_ptm a 
							set   thang_luong='2' 

								   , dthu_goi_goc = (select datcoc_csd_new from hocnq_ttkd.temp_ps where --dthu_tong>0 and 
																				ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm)
								  , dthu_goi = (select datcoc_csd_new from hocnq_ttkd.temp_ps where --dthu_tong>0 and 
								   											ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm)
								  , datcoc_csd= (select datcoc_csd_new from hocnq_ttkd.temp_ps where --dthu_tong>0 and 
																			ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm)
								   , thang_bddc= (select thang_bddc_new from hocnq_ttkd.temp_ps where --dthu_tong>0 and 
																			ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm)
								   , thang_ktdc= (select thang_ktdc_new from hocnq_ttkd.temp_ps where --dthu_tong>0 and 
																			ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm)
								   , sothang_dc= (select sothang_dc_new from hocnq_ttkd.temp_ps where --dthu_tong>0 and 
																			ma_gd=a.ma_gd and ma_tb =a.ma_tb  and thang_ptm=a.thang_ptm)
--								   , thang_tldg_dt='', thang_tlkpi='', thang_tlkpi_to='', thang_tlkpi_phong=''      
--				 select id, thang_ptm, thang_luong, thang_ptm, hdkh_id, thuebao_id, ma_tb,  dich_vu, tenkieu_ld, dthu_goi_goc, dthu_goi, thang_ptm, dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3 , lydo_khongtinh_dongia, doanhthu_dongia_nvptm, luong_dongia_nvptm, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to from ttkd_bsc.ct_bsc_ptm a
				where 
							exists  (select dthu_tong from hocnq_ttkd.temp_ps 
											where ma_gd = a.ma_gd and ma_tb =a.ma_tb and thang_ptm=a.thang_ptm) 
							and nvl(dthu_goi, 0) = 0
--						and ma_tb = 'hcm_ca_00067906'
;
commit;
rollback;

				select id, thang_ptm, thang_luong, thuebao_id, ma_tb,  dich_vu, tenkieu_ld, datcoc_csd, dthu_goi_goc, dthu_goi, thang_ptm, dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3
							, lydo_khongtinh_dongia, doanhthu_dongia_nvptm, luong_dongia_nvptm, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to 
				from ttkd_bsc.ct_bsc_ptm a
					where thang_luong = '2' 
				;

-- SMS Brandname: Tinh lai dthu goi, dthu_ps thang n va dthu_ps thang n+1
		drop table ttkd_bsc.tmp_bsc_smsbrn purge
		;
--		select * from ttkd_bsc.tmp_bsc_smsbrn;
		create table ttkd_bsc.tmp_bsc_smsbrn as
		    select thang_ptm, ma_gd, ma_tb, thuebao_id, nguon
						, dthu_ps, dthu_ps_n1
						 , dthu_goi dthu_goi_n, dthu_goi_ngoaimang dthu_goi_ngoaimang_n
						 , cast(null as number(12)) dthu_goi_n1, cast(null as number(12)) dthu_goi_ngoaimang_n1
						 , cast(null as number(12)) dthu_goi_bq, cast(null as number(12)) dthu_goi_ngoaimang_bq            
		    from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202501 and loaitb_id=131 ---thang n -1
		    ;


		update ttkd_bsc.tmp_bsc_smsbrn a 
			    set dthu_goi_ngoaimang_n = (select sum(nogoc) from bcss.v_tonghop@dataguard  --thang n- 1
															where phanvung_id=28 and ky_cuoc = 20250101 and khoanmuctc_id in (37,38,39,40,935,936,938,939,943,944,945,4097) and ma_tb=a.ma_tb)
				   ,dthu_goi_ngoaimang_n1= (select sum(nogoc) from bcss.v_tonghop@dataguard    ---thang n
                                                            where phanvung_id=28 and ky_cuoc = 20250201 and khoanmuctc_id in (37,38,39,40,935,936,938,939,943,944,945,4097) and ma_tb=a.ma_tb)
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
					  (select nvl(dthu_goi_bq,0)+nvl(dthu_goi_ngoaimang_bq,0) , dthu_goi_bq, dthu_goi_ngoaimang_bq, to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))		--thang n
						 from ttkd_bsc.tmp_bsc_smsbrn
						 where thuebao_id=a.thuebao_id)
			    where thang_ptm = 202501 and loaitb_id=131  --thang n-1
							and dthu_goi is null
			    ;
            commit;
            rollback;

-- Doi tuong KH:   chay cac th doituong_kh = null xy ly tiep                      
		update ttkd_bsc.ct_bsc_ptm 
						set doituong_kh=null 
--			select doituong_kh from ttkd_bsc.ct_bsc_ptm
		    where thang_ptm = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm'))
						and (dichvuvt_id!=2 or (dichvuvt_id=2 and loaitb_id<>21) ) and ma_kh<>'GTGT rieng' and doituong_kh is not null
		    ;
select khachhang_id from css_hcm.db_thuebao where ma_tb = 'hcm_edu_00009976';

			-- Update doi voi ds tbao VNPts, hoac ktra cac th ngoai le                  
				update ttkd_bsc.ct_bsc_ptm a
					   set doituong_kh = (case when-- (loaitb_id not in (20,149) and doituong_id in (374, 387, 361, 362))
																     
																    (loaitb_id in (20,149) and doituong_id in (1, 25))
																				then 'KHCN' 
																when loaitb_id in (20,149) and doituong_id in (2, 3, 4, 5, 6, 7, 10, 16, 18, 19, 21, 24) then 'KHDN'
--																when length(mst) >= 10 then 'KHDN'
														else null end)
--				  select thang_tldg_dt, thang_luong, thang_ptm, khachhang_id, ma_kh, ma_tb, ten_tb, doituong_id, doituong_kh, dich_vu, mst, loaitb_id from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm = 202502   --- thang n
							and nvl(ma_kh, 'abv') <> 'GTGT rieng' and doituong_kh is null
							and loaitb_id = 20-- and doituong_kh = 'KHCN' and doituong_id in (1, 25) and length(mst) >= 10
							and loaitb_id<>21
							and (lydo_khongtinh_luong is null or lydo_khongtinh_luong not like '%Chu quan khong thuoc TTKD%')
							and nvl(thang_tldg_dt, 999999) >= 202502
					   ;
			
			---cac th con lai
			update ttkd_bsc.ct_bsc_ptm a 
				   set doituong_kh = (case when (select c.khdn from css_hcm.db_khachhang b, css_hcm.loai_kh c 
																		where b.loaikh_id=c.loaikh_id and khachhang_id = a.khachhang_id) = 0 then 'KHCN'
															when (select c.khdn from css_hcm.db_khachhang b, css_hcm.loai_kh c 
																		where b.loaikh_id=c.loaikh_id and khachhang_id = a.khachhang_id) = 1 then 'KHDN'
															when loaitb_id = 271 then 'KHCN'
														--	when length(mst) >= 10 then 'KHDN'
															    else null end)
--			    	    select thang_tldg_dt, thang_luong, thang_ptm, khachhang_id, ma_tb, doituong_kh, dich_vu, dichvuvt_id, mst from ttkd_bsc.ct_bsc_ptm a
			    where (thang_ptm = 202502   --- thang n
								or thang_luong in (4))			---flag 4 file so 5 import dung thu chuyen dung that
					 and nvl(ma_kh, 'abv') <> 'GTGT rieng'  and doituong_kh is null 
				   and (dichvuvt_id !=2 or (dichvuvt_id=2 and loaitb_id<>21) )
				   and nvl(thang_tldg_dt, 999999) >= 202502
--				 and ma_duan_banhang = '241612'
				   ;
				   
--                         select thang_tldg_dt, thang_luong, thang_ptm, khachhang_id, ma_tb, doituong_kh, dich_vu, mst, ten_tb from ttkd_bsc.ct_bsc_ptm a
--			    where thang_ptm = 202501 and DOITUONG_KH = 'KHCN' and taxcheck(mst) = 1
              
    
		
		   ---Update cac th dau vao sai, update theo ten_tb
               update ttkd_bsc.ct_bsc_ptm a 
					set doituong_kh = 'KHDN'
--					     select chuquan_id, thang_tldg_dt, thang_ptm, khachhang_id, ma_kh, ma_tb, ten_tb, doituong_id, doituong_kh, dich_vu, mst, loaitb_id from ttkd_bsc.ct_bsc_ptm a
					    where thang_ptm = to_number(to_char(trunc(sysdate, 'month') - 1, 'yyyymm')) --thang n
										and doituong_kh = 'KHCN'
									   and (bo_dau(upper(ten_tb)) like 'CONG TY%' or bo_dau(upper(ten_tb)) like 'CTY%'  
											 OR bo_dau(upper(ten_tb)) like 'CN TONG CONG TY%' or bo_dau(upper(ten_tb)) like 'CHI NHANH%'
											 OR bo_dau(upper(ten_tb)) like 'NGAN HANG%' or bo_dau(upper(ten_tb)) like 'CHI CUC %' 
											 OR bo_dau(upper(ten_tb)) like 'VPDD%' OR bo_dau(upper(ten_tb)) like 'VAN PHONG DAI DIEN%' OR bo_dau(upper(ten_tb)) like 'VAN PHONG CONG CHUNG%'
											 OR bo_dau(upper(ten_tb)) like '%TR__NG PH_ TH_NG TRUNG H_C%'
											 OR bo_dau(upper(ten_tb)) like '%TR__NG TRUNG H_C PH_ TH_NG%'
											 OR bo_dau(upper(ten_tb)) like '%TR__NG TIEU H_C%'
											 OR bo_dau(upper(ten_tb)) like '%TOA AN NHAN DAN%'
											 OR bo_dau(upper(ten_tb)) like '%UY BAN NHAN DAN%'
											 OR bo_dau(upper(ten_tb)) like 'TRUNG TAM%'
											  OR bo_dau(upper(ten_tb)) like '%NGAN HANG CHINH SACH%')
			 ;

commit;
rollback;
			---khi có khieu nại--Bo sung thong tin QLDA
			---Bsung Thong tin QLDA
				UPDATE ttkd_bsc.ct_bsc_ptm a
							set ma_duan_banhang = (select replace(ma_duan, ' ', '') from css_hcm.hd_khachhang where hdkh_id = a.hdkh_id)
									, thang_luong = 44
--				select thang_luong, thang_ptm, ma_duan_banhang, hdkh_id from ttkd_bsc.ct_bsc_ptm a
					where exists (select 1 from css_hcm.hd_khachhang where MA_DUAN is not null 
																	and hdkh_id = a.hdkh_id
																	and nvl(regexp_replace(a.ma_duan_banhang, '\D', ''), 'r') <> regexp_replace (ma_duan, '\D', '')
--																	and nvl(a.ma_duan_banhang, 'r') <> replace(ma_duan, ' ', '')
												)
								and thang_ptm >= 202411
								and nvl(thang_tldg_dt, 999999) >= 202502
								and nvl(thang_tldg_dt_nvhotro, 999999) >= 202502			--- chua tinh luong
								and thang_luong not in (70, 71, 86, 87)			---loai tru cac ma_duan ct_ptm_ngoaictr_imp_insert
--								and ma_gd = '00967744'
--								and ma_tb = 'ravi217'
;



				;
				MERGE into ttkd_bsc.ct_bsc_ptm a
							using (with 
													yc_dv as (select ma_yeucau, id_ycdv, ma_dichvu, row_number() over(partition by MA_YEUCAU, MA_DICHVU order by NGAYCAPNHAT desc) rnk
																	from ttkdhcm_ktnv.amas_yeucau_dichvu
																	where MA_HIENTRANG <> 14
																	)
													, t as	 (select c.manv_presale_hrm, c.tyle/100 tyle_hotro, decode(tyle_am,0,1,c.tyle_am/100) tyle_am, d.loaitb_id_obss, b.ma_yeucau, b.ma_dichvu, c.tyle_nhom
																		, c.NGAYHEN, c.NGAYCAPNHAT, c.NGAYNHANTIN_PS, c.NGAYXACNHAN, c.ps_truong
																 from yc_dv b, ttkdhcm_ktnv.amas_booking_presale c, ttkdhcm_ktnv.amas_loaihinh_tb d
																					where b.ma_yeucau=c.ma_yeucau and b.id_ycdv=c.id_ycdv and b.ma_dichvu = d.loaitb_id
																								and c.tyle>0 and c.ps_truong=1 and c.xacnhan=1  
																)
												select MANV_PRESALE_HRM, LOAITB_ID_OBSS, MA_YEUCAU, MA_DICHVU, TYLE_AM--, sum(tyle_hotro) tyle_hotro, sum(TYLE_NHOM) TYLE_NHOM
																, decode(sum(ps_truong), 1, sum(tyle_hotro), max(tyle_hotro)) tyle_hotro
																, decode(sum(ps_truong), 1, sum(TYLE_NHOM), max(TYLE_NHOM)) TYLE_NHOM
															from t
--															where ma_yeucau  in (375340)
															group by MANV_PRESALE_HRM, LOAITB_ID_OBSS, MA_YEUCAU, MA_DICHVU, TYLE_AM
										) b
							ON (b.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', ''))
												and b.loaitb_id_obss = a.loaitb_id)
							WHEN MATCHED THEN
									UPDATE SET a.manv_hotro = b.manv_presale_hrm
															, a.tyle_hotro = b.tyle_hotro
															, a.tyle_am = b.tyle_am
															, thang_luong = case when thang_luong in (70, 71, 86, 87) then thang_luong else 4 end
															
--					  select thang_luong, thang_ptm, manv_hotro, tyle_hotro, tyle_am, ma_duan_banhang, dich_vu, loaitb_id, ma_gd, nguon, thang_tldg_dt_nvhotro, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a
							where ma_duan_banhang is not null and tyle_am is  null
--										and thang_tldg_dt_nvhotro is null and thang_tldg_dt is null
										and (thang_ptm = 202502		---thang n - 3
													or thang_luong in (44, 70, 71, 86, 87))	--Flag nay update thong tin NV PGP hotro
										 and exists (select distinct c.manv_presale_hrm, c.tyle/100, b.ma_yeucau, d.loaitb_id_obss
															from ttkdhcm_ktnv.amas_yeucau_dichvu b, ttkdhcm_ktnv.amas_booking_presale c, ttkdhcm_ktnv.amas_loaihinh_tb d
															where b.ma_yeucau = c.ma_yeucau and b.id_ycdv = c.id_ycdv and b.ma_dichvu = d.loaitb_id
																	  and c.tyle>0 and ps_truong = 1 and xacnhan = 1    
																	  and b.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', ''))
																	  and d.loaitb_id_obss = a.loaitb_id 
															) 
--															and ma_gd = '01096492'
--										and id in (9108636, 9106564)
--									and ma_duan_banhang = '375340';
--										and thang_luong = 86
--		and ma_gd in ('00984910', '00989674','HCM-LD/02008921','00989706', '00989693', 'HCM-LD/02008921')
								;
			-----END bo sung thong tin QLDA
commit;
-- QLDA: xoa , chay lai
delete from ttkd_bsc.ct_bsc_ptm_kiemtraduan where thang=202502 and ptm_id = 10874335;ma_duan_banhang = '340701';ma_duan_banhang = '202655'; and ma_tb = 'hcm_ca_00103739';and ma_tb = 'hcm_colo_00009265';
		;
		select *   from ttkd_bsc.ct_bsc_ptm_kiemtraduan a where thang = 202502 and ma_duan_banhang = '10874335';227188; and ma_duan_banhang = '227812'; and ma_tb = 'hcm_ca_00103739'; and ma_gd = 'HCM-TD/00690780';hcm_hddt_inbot_00001044
			insert into ttkd_bsc.ct_bsc_ptm_kiemtraduan
								(thang, ptm_id, thang_ptm, ma_gd, ma_tb, dich_vu, ma_duan_banhang, mst, mst_tt
								, dichvuvt_id, loaitb_id, ma_yeucau, duan_daduyet, duan_mst, kt_loaitb_id, kt_mst, kt_nghiemthu
								)
					
			    select to_char(trunc(sysdate, 'month') - 1, 'yyyymm') thang, a.id, a.thang_ptm, a.ma_gd, a.ma_tb, a.dich_vu, a.ma_duan_banhang, a.mst, a.mst_tt, a.dichvuvt_id, a.loaitb_id, b.ma_yeucau, b.daduyet, b.masothue
						 , (case when exists (select c.ma_yeucau, LOAITB_ID_OBSS from ttkdhcm_ktnv.amas_yeucau_dichvu c, ttkdhcm_ktnv.amas_loaihinh_tb d
																  where c.ma_dichvu=d.loaitb_id 
																				and c.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', ''))
																				---and MA_HIENTRANG = 12 		---cho ktra duyet TTKD vb
																				and d.loaitb_id = a.loaitb_id
																	) 
														then 1 
--										when exists(select * from ttkd_bsc.map_loaihinhtb where loaitb_id=a.loaitb_id and loaitb_id_qlda=b.ma_dichvu) 
--											    then 1 
										else 0 end 
							  ) kt_loaitb_id
						 , (case when regexp_replace (replace(a.mst, '-000', ''), '\D', '') = regexp_replace (b.masothue, '\D', '') 
--																or regexp_replace (mst_tt, '\D', '')=regexp_replace (b.masothue, '\D', '') 
												--substr(regexp_replace (replace(a.mst, '-000', ''), '\D', ''), 1, 10) = substr(regexp_replace (b.masothue, '\D', ''), 1, 10) 
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
--			    select a.loaitb_id, mst, thang_ptm, ma_tb, ma_duan_banhang, thang_luong, id
			    from ttkd_bsc.ct_bsc_ptm a, ttkdhcm_ktnv.amas_yeucau b
			    where (a.thang_ptm = 202502			--- thang n
								or a.thang_luong in (4, 44, 70, 71, 86, 87))			---flag 4, 44 file so 5 import dung thu chuyen dung that & bsung thong tin QLDA
						and nvl(thang_tldg_dt, 999999) >= 202502
						and dichvuvt_id <> 2                  --thuebao_id is not null 
						and a.doituong_kh='KHDN' and nhom_tiepthi in (1, 3)		---xet du an doi voi TTKD
						and loaitb_id != 149 									----VCC tren CCBS khong co nhap dc QLDA
						and  to_number(regexp_replace (a.ma_duan_banhang, '\D', '')) = b.ma_yeucau (+)
					and  not exists (select * from ttkd_bsc.ct_bsc_ptm_kiemtraduan where thang = a.thang_ptm and PTM_ID = a.id) 
--			and ma_duan_banhang = '375340'
--		and id = 10874335
				; 
				commit;
        rollback;
        
-- ket qua kiem tra du an: neu khong hop le thi giam dthu quy doi 50%
			update ttkd_bsc.ct_bsc_ptm_kiemtraduan a
			set kiemtra_duan = (
													with kq as (
															  select ptm_id, ma_duan_banhang, ma_yeucau, duan_daduyet, kt_mst, kt_loaitb_id
																		   , case when ma_duan_banhang is null then '; Ho so khong nhap ma du an' end kq1
																		   , case when ma_duan_banhang is not null and ma_yeucau is null then '; Ma du an '||ma_duan_banhang||' chua dang ky tren QLDA' end kq2
--																		   , case when ma_yeucau is not null and kt_nghiemthu = 0 then '; Ma du an '||ma_duan_banhang||' chua nghiem thu tren QLDA' end kq3			---cho vb trien khai TTKD
																		   , case when ma_yeucau is not null and (duan_daduyet is null or duan_daduyet <> 1) then '; Du an chua duoc duyet' end kq4
																		   , case when ma_yeucau is not null and kt_mst=0 then '; Khong dung KH (mst)' end kq5
																		   , case when ma_yeucau is not null and nvl(kt_loaitb_id, 0)=0 then '; Khong dung dich vu dang ky' end kq6
															  from ttkd_bsc.ct_bsc_ptm_kiemtraduan
															  where thang = to_char(trunc(sysdate, 'month') - 1, 'yyyymm')
															)                                        
											  select nvl(c.kq1,'') || nvl(c.kq2,'')-- || nvl(c.kq3,'') 
															|| nvl(c.kq4,'') || nvl(c.kq5,'') || nvl(c.kq6,'')--, ma_duan_banhang
											  from  kq c
											  where c.ptm_id = a.ptm_id
										) 
--			select *  from ttkd_bsc.ct_bsc_ptm_kiemtraduan a
			where thang = 202502 
--						and ma_duan_banhang = '375340'
--			and ma_duan_banhang = '279544'
--			and ma_duan_banhang = '349740'

			    ;
    
			update ttkd_bsc.ct_bsc_ptm a 
				   set kiemtra_duan = (select nvl(kt_nghiemthu, 0) || kiemtra_duan from ttkd_bsc.ct_bsc_ptm_kiemtraduan 
																							where thang = 202502 and ptm_id=a.id)
																
--		 select thang_luong, thang_ptm, kiemtra_duan, thang_tldg_dt, thang_tlkpi, nguon, ma_duan_banhang, ma_gd,  ma_tb, mst from ttkd_bsc.ct_bsc_ptm a 
			    where exists(select 1 from ttkd_bsc.ct_bsc_ptm_kiemtraduan where thang = 202502 and ptm_id=a.id)
--		and ma_duan_banhang = '375340'
--						and ma_tb = 'hcm_colo_00009265'

			    ;

		commit;
		rollback;
          
-- KH hien huu - KH moi:   
			---dot 2  khong xoa
			update ttkd_bsc.ct_bsc_ptm set khhh_khm='' 		
	--   select khhh_khm from ttkd_bsc.ct_bsc_ptm
			    where thang_ptm = 202502 and khhh_khm is not null
--			    and ma_tb = '84917720053'
			    ;
		update ttkd_bsc.ct_bsc_ptm a 
			    set khhh_khm = 'KHHH'
--		   select khhh_khm, doituong_kh, nguon, tenkieu_ld from ttkd_bsc.ct_bsc_ptm
			    where thang_ptm = 202502 --- thang n
								and khhh_khm is null 
							   and nguon in ('thaydoitocdo','tailap') 
				   ;
			update ttkd_bsc.ct_bsc_ptm set khhh_khm = 'KHM' 
--		   select khhh_khm, dichvuvt_id, doituong_kh from ttkd_bsc.ct_bsc_ptm
			    where thang_ptm = 202502 --- thang n
							and khhh_khm is null 			--loai tru da gan roi, khong gan lai
							and (nguon='web Digishop' 
										 or (doituong_kh='KHCN'
											    and (dichvuvt_id!=2 or dichvuvt_id is null)
											    and nguon in ('ptm_codinh','ccq','dt_ptm_vnp','shop_hcm_mytv_online')
											)
									)
						   
				   ; 
  
				   
				---chay khi co file giao ---toi uu lai
			update ttkd_bsc.ct_bsc_ptm a 
				   set khhh_khm = 'KHHH'
--		  select khhh_khm, loaitb_id, ma_duan_banhang, mst from ttkd_bsc.ct_bsc_ptm a
			    where a.thang_ptm = 202502 --- thang n
					and doituong_kh='KHDN' and mst is not null --thang n
					  and exists (select mst_chuan from ttkd_bct.db_thuebao_ttkd
												   where ma_dt_kh='dn' and cvnv is null and tb_dacbiet is null 
																  and trangthaitb_id not in (7,8,9) and to_number(to_char(ngay_sd,'yyyymm')) < a.thang_ptm			--- thang n
																  and to_number(regexp_replace (mst, '\D', ''))=to_number(regexp_replace (a.mst, '\D', ''))
											)
									
--					  and ma_tb = '84916665480'
					  ;
                                              
					update ttkd_bsc.ct_bsc_ptm a 
					   set khhh_khm =  'KHHH'
--					      select khhh_khm, doituong_kh, dich_vu, ten_tb, mst, so_gt from ttkd_bsc.ct_bsc_ptm a
				    where a.thang_ptm = 202502 and doituong_kh='KHDN'
						  and nvl(khhh_khm, 'KHM') = 'KHM' and nvl(loaitb_id, 21) !=21
				---hoac 1
--						and   exists (select 1 from ttkd_bct.db_thuebao_ttkd
--															   where ma_dt_kh='dn' and cvnv is null and tb_dacbiet is null                                                                         
--																  and trangthaitb_id not in (7,8,9) and to_number(to_char(ngay_sd,'yyyymm')) < a.thang_ptm		--thang n
--																  and lower(so_gt) = '0'||lower(a.so_gt)
----																and	 lower(so_gt) = lower(a.so_gt)
--												  )
			---hoac 2
						and  exists (select 1 from ttkd_bct.db_thuebao_ttkd
										   where ma_dt_kh='dn' and cvnv is null and tb_dacbiet is null 
											  and trangthaitb_id not in (7,8,9) and to_number(to_char(ngay_sd,'yyyymm')) < a.thang_ptm		--thang n
											  and khachhang_id=a.khachhang_id
											  )          
					;
					
					update ttkd_bsc.ct_bsc_ptm a 
					   set khhh_khm =  'KHM'
--					      select khhh_khm, doituong_kh, ma_tb, ten_tb, mst, nguon, ghi_chu, tenkieu_ld, loaitb_id from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm = 202502 	--thang n
									and doituong_kh = 'KHDN' --and mst is not null
								  and khhh_khm is null and nvl(loaitb_id, 21) !=21    
								  	
					;
				update ttkd_bsc.ct_bsc_ptm set khhh_khm='KHM' 
--		   select khhh_khm, thang_ptm, dichvuvt_id, doituong_kh, ten_tb, loaitb_id, nguon from ttkd_bsc.ct_bsc_ptm
			    where thang_ptm = 202502 --- thang n
							and khhh_khm is null 			--loai tru da gan roi, khong gan lai
							and (nguon='dt_ptm_vnp' 
										 and doituong_kh='KHCN'
									)
						   
				   ; 
					
					select khhh_khm, doituong_kh, ma_tb, nguon, ghi_chu, tenkieu_ld, dichvuvt_id from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm = 202502		--thang n
--									and doituong_kh = 'KHDN' and mst is not null
								  and khhh_khm is null and nvl(loaitb_id, 21) !=21 
								  ;
				commit;
				rollback;
                    
-- Dia ban:        vi tri ban hang truc tiep moi xet trong/ngoai dia ban
			---VTTP, PotMasCo, BHOL khong xet trong/ngoai chuong trinh
			--- GIA HAN INTERNET truc tiep khong xet khong xet trong/ngoai chuong trinh
			update ttkd_bsc.ct_bsc_ptm a 
						set diaban =null
	---	select diaban from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202502 and (loaitb_id<>21 or ma_kh='GTGT rieng') and diaban is not null
			    
			    ;
          
				update  ttkd_bsc.ct_bsc_ptm a
				    set diaban = (case 
											when nhom_tiepthi in (2, 11) or manv_ptm like 'POT%' then 'Khong xet trong/ngoai CT ban hang'			---TTVT, Potmasco
											when ma_pb = 'VNP0703000' then 'Khong xet trong/ngoai CT ban hang'				---PBHOL khong xet
											when loaitb_id in (21, 20, 149) then 'Khong xet trong/ngoai CT ban hang'				----VNPTS, VCC tren CCBS khong co nhap dc QLDA
											when loaitb_id in (134) and nguon = 'ct_ptm_ngoaictr_imp_insert' then 'Khong xet trong/ngoai CT ban hang'				----Gia han INT khong xet QLDA, chua co Quy dinh, ap dung doi danh sach a Nghia
--											  when ma_vtcv not in ('VNP-HNHCM_BHKV_6','VNP-HNHCM_BHKV_6.1',
--																		    'VNP-HNHCM_BHKV_17','VNP-HNHCM_BHKV_42','VNP-HNHCM_KHDN_3','VNP-HNHCM_KHDN_3.1',
--																		    'VNP-HNHCM_KHDN_4','VNP-HNHCM_BHKV_41',
--																		    'VNP-HNHCM_KHDN_18', 'VNP-HNHCM_KTNV_8') then 'Khong xet trong/ngoai CT ban hang'
											  when doituong_kh = 'KHCN' then 'Trong CT ban hang'
--											  when doituong_kh = 'KHDN' and subtr(kiemtra_duan, 2) = '1' then 'Trong CT ban hang'			---wait vb trien khai TTKD
											  when doituong_kh = 'KHDN' and substr(kiemtra_duan, 2) is null then 'Trong CT ban hang'
											  when doituong_kh = 'KHDN' and kiemtra_duan is not null then 'Ngoai CT ban hang'
											  else null end)
--							select thang_luong, thang_ptm, thuebao_id, ma_tb, dichvuvt_id, loaitb_id, dich_vu, mst, diaban, nhom_tiepthi, ma_vtcv, doituong_kh, kiemtra_duan, ma_duan_banhang, manv_ptm, nguon, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a
				    where (a.thang_ptm = 202502 --- thang n
								or a.thang_luong in (4, 44, 70, 71, 86))			---flag 4, 44 file 6 kiem tra duan sau khi da update QLDA va ctr ngoai tinh bsung thang_luong = 4, 44, 70, 71, 86
						and (loaitb_id<>21 or ma_kh='GTGT rieng')
				
					   and nvl(thang_tldg_dt, 999999) >= 202502 --and nvl(thang_tlkpi_phong, 999999) >= 202501  --lay cac tbao chua chi dektra duan
--					   and  exists (select * from ttkd_bsc.nhanvien where thang = 202501 and donvi = 'VTTP' and a.manv_ptm = ma_nv)
--				and id in (9763042, 9763041, 9618083);
			
					   ;

commit;
rollback;
-- Xet mien HS goc doi voi hop dong chuyen doi, nang cap, va lap moi cdbr/tsl co tra truoc      
			update ttkd_bsc.ct_bsc_ptm set mien_hsgoc='' 
--		select * from ttkd_bsc.ct_bsc_ptm
			    where thang_ptm = 202502 and dichvuvt_id not in (2,13,14,15,16) and loaitb_id not in (21,172) and mien_hsgoc is not null
			    ;
                               
			update ttkd_bsc.ct_bsc_ptm a 
				   set mien_hsgoc = 1 
--		 select * from ttkd_bsc.ct_bsc_ptm
			    where thang_ptm = 202502 --- thang n
							and mien_hsgoc is null 
						   and ( (dichvuvt_id not in (2,13,14,15,16) and (loaihd_id is null or chuquan_id in (266, 264))  ---Newlife, VTH1 hoac hok co hop dong --> mien hso goc
										)
											--or ( dichvuvt_id=4 and sothang_dc>=6)                                   -- Fiber, MyTV dong truoc truoc >=6 thang  
											or ( thang_ptm = 202502 and loaitb_id in (15,17) and thuebao_cha_id is not null)       -- isdn, thue bao luong (so con)
											or ID_447 is not null  -- hop dong tren ID 447 khong kiem tra hso goc
								)
									
							;
                  commit;
				rollback;

-- He so dich vu:    
		---tao bang TEMP bat buoc
		drop table ttkd_bsc.x_temp purge;
		---thang n
		create table ttkd_bsc.x_temp as select * from ttkd_bct.ptm_codinh_202502
		;
		
		update ttkd_bsc.ct_bsc_ptm a 
		    set heso_dichvu = case when loaitb_id=61
																   then
																	  case when loaihd_id=1 and goi_id is null then 1.1                               -- MyTV ko tham gia goi tich hop => 1.1
																			 when loaihd_id<>1 and kieuld_id=96 then 0.3                             -- MyTV tai lap
																		 else 1 end        
												 when ma_pb in ('VNP0701100','VNP0701200','VNP0701300','VNP0701400','VNP0701500','VNP0701600','VNP0701800','VNP0702100','VNP0702200')
														 and loaitb_id=58 and loaihd_id=1 and goi_id is null                         -- 9PBHKV lap moi Fiber khong tham gia goi tich hop
													  then 1.1
										  
--                                              when loaitb_id = 271 and nvl(sothang_dc, 0) < 1 then 0                         -- MyTV OTT: <6t : 0.2 ; tu 6t-> duoi 12t: 0.3 ; tu 12t tro len: 0.4
									when loaitb_id = 35 and ma_tb = 'hcm_ioff_00000115' then 0.3 			--vb 1407- DN3, eo	660772 den het thang 6/2025
                                            when loaitb_id=200 then    -- Ecabinet
													   case when sothang_dc<3 or sothang_dc is null then 0.1
															  when sothang_dc>=3 and sothang_dc<6 then 0.2
															  when sothang_dc>=6 and sothang_dc<12 then 0.25
															  when sothang_dc>=12 then 0.35 
															  else 0 
													   end
                                            when loaitb_id in (35,121,120) then      -- eGov (VNPT iGate (121), iOffice (35), VNPT Portal (120), ...) : hop dong theo thang, tra sau (hinh thuc cho thue) => hsdv =1;  hop dong tron goi, tra truoc => hsdv = 0.4
															   case when nvl(datcoc_csd,0)>0 
																								  and (select nvl(muccuoc_tb,0) from ttkd_bsc.x_temp
																												where thuebao_id=a.thuebao_id and hdtb_id=a.hdtb_id
																										)>=0                                                
																		    then 0.4  -- thue thang , nhung co dat coc => tinh nhu dthu tron hop dong
																	  when nvl(datcoc_csd,0)=0
																							  and (select nvl(muccuoc_tb,0) from ttkd_bsc.x_temp
																											where thuebao_id=a.thuebao_id and hdtb_id=a.hdtb_id
																									) > 0                                     
																		    then 1  
																	else 0.4		---Update 4/6/24 thay the FRAME muctb = 0
																  end
                                                when loaitb_id in (11,58,61,210) and kieuld_id=96 then 0.3      -- tai lap
                                                when loaitb_id=153 and loaihd_id=41 then 
														case when sothang_dc>=6 then 0.3 else 0 end                     -- Gia han VNPT SmartCloud theo VB 167/TTr-NS-DH 23/05/2022: chi ghi nhan khi gia han goi 6 thang tro len
                                                when loaitb_id=296 then                                                                -- VNPT Home-Clinic: theo thang : 1, theo g�i 6t,12t: 0.3 , VB 328/TTKD HCM-DH 31/12/2021
															case when sothang_dc>=6 then 0.3 else 1 end
                                                when loaitb_id in (317, 287, 285, 279, 136) then                                         -- VNPT AntiDDoS: theo thang =1, theo thu� dich vu 72 gio = 0.3, eoffice 718660 
                                                                                                                                                                    -- VNPT IOC (Trung tam Dieu hanh thong minh), VNPT eDIG (Phan mem He thong quan ly Ho so)
																																    -- VNPT HIS
														  case when nvl(datcoc_csd, 0) > 0 
																					and (select nvl(muccuoc_tb, 0) from ttkd_bsc.x_temp
																								 where thuebao_id = a.thuebao_id and hdtb_id = a.hdtb_id
																							) >=0
																				then 0.3 
																    when nvl(datcoc_csd,0) = 0 
																					and (select nvl(muccuoc_tb, 0) from ttkd_bsc.x_temp
																								 where thuebao_id = a.thuebao_id and hdtb_id = a.hdtb_id
																							) >= 0
																					then 1
																	else 0.3
														   end			-- thue phan mem theo thang: 1 ; Mua tron goi phan mem: 0.3
--										when loaitb_id = 20 and VNP_MOI = 'MNP' then 1.5	---thue bao MNP
--										when loaitb_id = 20 and kieuld_id = 2 then (select heso_dichvu from ttkd_bsc.dm_loaihinh_hsqd b where a.loaitb_id=b.loaitb_id) --Hoa mang moi

									---VNPT eKyc khong co hop dong tra truoc (tra 1 lan) tinh heso_dv = 1
									when loaitb_id in (276) and nvl(datcoc_csd, 0) = 0 then 1
                                            else (select heso_dichvu from ttkd_bsc.dm_loaihinh_hsqd b where a.loaitb_id=b.loaitb_id)                       
									end
						  , heso_dichvu_1 = case when loaitb_id = 131 then 0.004 end  
--				     select thang_luong, thang_ptm, thang_tldg_dt, heso_dichvu_1, heso_dichvu, nguon, dich_vu, loaitb_id, loaihd_id, ma_tb, datcoc_csd, sothang_dc from ttkd_bsc.ct_bsc_ptm a
				    where 
						(thang_ptm = 202502 --- thang n
								or thang_luong in (2))			---flag 2 update Dthu_goi = datcoc_csd file 6
							and loaitb_id  not in (20, 21)
							and nguon not like '%ct_ptm_ngoaictr_imp%' and id_447 is null --and heso_dichvu is null
					
				    ; 
				commit;
				rollback;
				select * from ttkd_bsc.dm_loaihinh_hsqd
                ;
/*-- Luu y SMS Brandname neu ghi nhan theo gia tri hop dong => pbh gui ds thue bao ve PDH duyet
update ct_bsc_ptm a  
    set  heso_dichvu = (case when dthu_goi >=1000000000 then 0.05 else 0.08 end)
           ,heso_dichvu_1=0.004
*/
  
                                
-- He so khu vuc dac thu: danh muc quan ly thay doi from PDH.
--			delete ttkd_bsc.dm_duan_dacthu where thang=202502
			;
			select * from ttkd_bsc.dm_duan_dacthu where thang=202502
			;
			
			insert into ttkd_bsc.dm_duan_dacthu 
					    select 202502 thang, ma_da, ten_duan, heso_kvdacthu, ttvt_id, ghichu, diachi
					    from ttkd_bsc.dm_duan_dacthu a
					    where not exists (select 1 from ttkd_bsc.dm_duan_dacthu where thang = 202502 and ma_da=a.ma_da)    
							  and thang = 202501
							  ;
			
			update ttkd_bsc.ct_bsc_ptm a 
			    set heso_kvdacthu = case when ma_da is null then 1
															when ma_da is not null and exists (select 1 from ttkd_bsc.dm_duan_dacthu where thang = a.thang_ptm and ma_da=a.ma_da)
																	then (select heso_kvdacthu from ttkd_bsc.dm_duan_dacthu where thang = a.thang_ptm and ma_da=a.ma_da)
															else 1 end
--				select thang_luong, thang_ptm, nguon, heso_kvdacthu, ma_da, ma_tb from ttkd_bsc.ct_bsc_ptm a
			    where a.thang_ptm = 202502--- thang n
							and loaitb_id not in (21)
--							   and heso_kvdacthu is null
--			and ma_tb = 'vcam0006684'
		
				   ;
                              commit;

-- He so tra truoc: 
		update ttkd_bsc.ct_bsc_ptm a 
				set heso_tratruoc=null 
	---	select nguon, heso_tratruoc from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202502 and loaitb_id!=20 and nguon not like 'ct_ptm_ngoaictr%'  --loai tru cac file tu excel A Nghia
			   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id)
			   ;
 
            
			update ttkd_bsc.ct_bsc_ptm a 
			    set heso_tratruoc = (case when loaitb_id = 271 and sothang_dc >= 12 then 0.4			----vb 353 heso tra truoc MyOTT
															when loaitb_id = 271 and sothang_dc >= 6 and sothang_dc < 12 then 0.3 ----vb 353 heso tra truoc MyOTT
															when loaitb_id = 271 and sothang_dc < 6 then 0.2	----vb 353 heso tra truoc MyOTT
															when not exists (select * from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id)
																then 1
															when sothang_dc > 16 then 1.2
															when sothang_dc >= 12 and sothang_dc <= 16 then 1.15
															when sothang_dc >= 6 and sothang_dc < 12 then 1.1
															when sothang_dc is not null and sothang_dc < 6 then 1
															
												   else 1 end)
--				select thang_luong, thang_ptm, ma_tb, thang_tldg_dt, sothang_dc, heso_tratruoc, dich_vu, dichvuvt_id, loaitb_id from ttkd_bsc.ct_bsc_ptm a
			    where 
							(thang_ptm = 202502 --- thang n
								or thang_luong in (2))			---flag 2 update Dthu_goi = datcoc_csd file 6 thang_luong = 2, 87
							and loaitb_id not in (20, 21) 	---VNPts anh Tuyen da tinh
							and nvl(thang_tldg_dt, 999999) >= 202502
--							thang_luong = 455
							
				   ;

commit;
rollback;
   
-- He so dai ly :  VB 353/TTr-DH-NS - 12/2023:    AM ban hang thong qua Dai ly tinh 5%; nguoc lai AM QLDL ban hang thong qua DAI LY 0%
			---88/TTr-?H-NS-KTKH ng�y ng�y 12/9/2022 
			--- Neu trong thang xuat hien Ma Dai ly moi so voi tap chot Dai ly thang truoc
					--- neu Dai ly nay do AM thuong (khac AM QLDL)  ho tro --> 5% * Dthu goi * heso_hotro_nvptm, va PreSale = 5% * Dthu goi * heso_hotro_nvhotro
					--- Neu AM QLDL hotro -> 0% DThu goi, va PreSale = 0% dthu goi
			--- Nguoc lai Neu Dai ly hien huu ton tai tap chot Daly ly thang truoc
					--- 0% AM, va PS tinh binh thuong nhu thue bao khong qua DAILY PTM
			update ttkd_bsc.ct_bsc_ptm a 
			    set heso_daily = ''
			    where thang_ptm = 202502 and heso_daily is not null
			    ;
   
    select *  from ttkd_bsc.dm_daily_khdn where thang = 202502;
    
			update ttkd_bsc.ct_bsc_ptm a 
			    set heso_daily = 
											case when exists (select 1  from ttkd_bsc.dm_daily_khdn where thang = 202502 and thang_kyhd < a.thang_ptm and ma_daily = a.ma_nguoigt) ---Dai ly hien huu
																then 0
													when exists (select 1  from ttkd_bsc.dm_daily_khdn where thang = 202502 and  thang_kyhd = a.thang_ptm and ma_daily = a.ma_nguoigt) ---Dai ly moi
																	and ma_vtcv in ('VNP-HNHCM_KHDN_23')		---AM QLDL ho tro
																then 0
													when exists (select 1  from ttkd_bsc.dm_daily_khdn where thang = 202502 and  thang_kyhd = a.thang_ptm and ma_daily = a.ma_nguoigt) ---Dai ly moi
																	and ma_vtcv not in ('VNP-HNHCM_KHDN_23')		---AM thuong ho tro
																then case when loaitb_id not in (149) then 0.05 else 0 end---ngoai tru dvu VCC (VNPts), xem xet khi co to trinh LD TTKD
												
													else null
											end
										
--						, thang_luong = 25
--		 select thang_luong, thang_ptm, manv_ptm, ma_vtcv, ma_nguoigt, heso_daily, thang_tldg_dt, thang_tlkpi_phong, ma_tiepthi, loaitb_id from ttkd_bsc.ct_bsc_ptm a
			    where (thang_ptm = 202502 --and manv_ptm is not null-- 
								or thang_luong in (4)		---file 6 update ma_vtcv
								)
							   and (upper(ma_tiepthi)like'GTGT_%' or upper(ma_tiepthi)like'DAILY_%' or upper(ma_tiepthi)like'DL_%'
												or upper(ma_nguoigt)like'GTGT_%' or upper(ma_nguoigt)like'DAILY_%' or upper(ma_nguoigt)like'DL_%'
									 )
							  -- and ma_vtcv  not in ('VNP-HNHCM_KHDN_3.1')
							   and nvl(thang_tlkpi_phong, 999999) >= 202502
							   
			
				   ;
				   
   
commit;
rollback;
 
-- HE SO QUY DINH: ban hang ngoai dia ban 
		-- 50% ap dung nvien (khong ap dung BHOL);
		-- Dthu KPI: khong ap dung he so nay	(ap dung PTM T8/2024)
		-- Dthu dongia: ap dung heso AM, khong ap dung cac vi tri (ap dung tu PTM T8/2024)
		-- 100% trong ctr va ap dung To, Phong
		-- Ban cheo khong xet QLDA
		-- VNPTS khong xet QLDA
		-- CCQ khong xet QLDA
		-- Fiber Newlife khong xet QLDA
		-- tai lap sau 35 ngay khong xet QLDA
		-- Khoi phuc Tly khong xet QLDA
		-- Lap moi  dvu DHSXKD, nang cap Fiber, MyTV, TSL, Colo xet QLDA
		-- Gia han GTGT/CNTT xet QLDA
		-- Gia han Inter trực tiêp, TSL khong xet QLDA, vi chua yêu cầu nhập QLDA
			update ttkd_bsc.ct_bsc_ptm 
					set heso_quydinh_nvptm = null
			    where thang_ptm = 202502 and (loaitb_id<>21 or ma_kh='GTGT rieng') 
			    ;
                                

				update ttkd_bsc.ct_bsc_ptm a
				    set heso_quydinh_nvptm = case when chuquan_id = 264 or ma_kh='GTGT rieng' or nhom_tiepthi in (2, 11) or manv_ptm like 'POT%' then 1
																			  else case when diaban in ('Trong CT ban hang','Khong xet trong/ngoai CT ban hang') then 1
																							when diaban='Ngoai CT ban hang' then 0.5 else 1 end
														   end
						, heso_quydinh_nvhotro = 1
						, heso_quydinh_dai = 1
				
--				  select nguon, thang_luong, thang_ptm, ma_kh, ma_tb, heso_quydinh_nvptm, heso_quydinh_nvhotro, heso_hotro_nvhotro, manv_hotro, manv_ptm, heso_quydinh_dai, manv_tt_dai, diaban, chuquan_id, nhom_tiepthi, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a    
				    where 
								(thang_ptm = 202502 --- thang n
										or thang_luong in (4))			---flag 4 file so 6 sau khi update QLDA thang_luong = 4, 87
					and (loaitb_id<>21 or ma_kh='GTGT rieng') --and heso_quydinh_nvhotro is null
					and nvl(thang_tldg_dt, 999999) >= 202502 
--				    and a.ma_duan_banhang = '152853'
--				    and thang_luong in ( 86)
--				and ma_duan_banhang in ('185813', '185810', '265802') and ma_gd in ('00901934', '00898044', '00897956')	
			
				    ;
                                    
commit;
rollback;
     
-- he so vtcv: cac vtcv duoc tinh = 1, ngoai tru ca th dac biet duyet qua Mail se dieu chinh lai heso (vi du duyet chi phi TTKD tu mail Chu Minh KHKT)
				update ttkd_bsc.ct_bsc_ptm 
					   set heso_vtcv_nvptm='', heso_vtcv_dai='', heso_vtcv_nvhotro=''
				    where thang_ptm=202502 and (loaitb_id<>21 or ma_kh='GTGT rieng')
				    ;
             
 
				update ttkd_bsc.ct_bsc_ptm a 
					   set   heso_vtcv_nvptm = 1
--																	case when chiphi_ttkd > 0 and thang_xn_chiphi_ttkd is not null then 0.5 else 1 end	---mail Chu Minh KHKT duyet chi phi TTKD (neu co)
							 ,heso_vtcv_nvhotro = 1
--														case when chiphi_ttkd > 0 and thang_xn_chiphi_ttkd is not null then 0.5 else 1 end	---mail Chu Minh KHKT duyet chi phi TTKD (neu co)
							 ,heso_vtcv_dai = 1
--												case when chiphi_ttkd > 0 and thang_xn_chiphi_ttkd is not null then 0.5 else 1 end	---mail Chu Minh KHKT duyet chi phi TTKD (neu co)
--				    select thang_luong, thang_ptm, chiphi_ttkd, thang_xn_chiphi_ttkd, heso_vtcv_nvptm, heso_vtcv_nvhotro, heso_vtcv_dai, manv_hotro, manv_tt_dai, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a 
				    where (thang_ptm = 202502		--- thang n
									or thang_luong in (44))		---flag 87 file Update anh Nghia thang_luong in (44, 87)
					and (loaitb_id<>21 or ma_kh='GTGT rieng') 
					and nvl(thang_tldg_dt, 999999) >= 202502

				    ;
             
    commit;

-- He so khach hang: 
    -- cdbr+gtgt
			update ttkd_bsc.ct_bsc_ptm a set phanloai_kh = null
	---  select phanloai_kh from ttkd_bsc.ct_bsc_ptm a 
			    where  thang_ptm = 202502 and phanloai_kh is not null-- and thang_luong = 86
			    ; 

			drop table hocnq_ttkd.temp_plkh purge
			;
			select * from hocnq_ttkd.temp_plkh;
			create table hocnq_ttkd.temp_plkh as
			    with plkh as (select KY_XACLAP, khachhang_id, PLKH_ID, ma_plkh, row_number() over (partition by khachhang_id order by KY_XACLAP desc) rnk
														from ttkd_bct.dbkh_plkh pl, css_hcm.phanloai_kh plkh 
														where pl.plkh_id=plkh.phanloaikh_id --and khachhang_id = 15572
										)
						, dbtb as (select khachhang_gom_id, KHACHHANG_ID_GOC, row_number() over (partition by KHACHHANG_ID_GOC order by khachhang_gom_id desc) rnk
												from ttkd_bct.db_thuebao_ttkd
												)
						, ptm as (select a.khachhang_id, b.khachhang_gom_id khachhang_id_plkh, row_number() over (partition by a.khachhang_id order by b.khachhang_gom_id desc) rnk
											 from ttkd_bsc.ct_bsc_ptm a
															join dbtb b on b.rnk = 1 and b.KHACHHANG_ID_GOC = a.khachhang_id-- and a.khachhang_id = 15572
											 where thang_ptm = 202502 
										)
			    select a.*, plkh.ma_plkh phanloai_kh
				   from  ptm a
							 join plkh on a.khachhang_id_plkh = plkh.khachhang_id and plkh.rnk = 1
					where a.rnk = 1 --and a.khachhang_id= 15572
			;
			create index hocnq_ttkd.idx_plkhid on hocnq_ttkd.temp_plkh (khachhang_id)
			;
             
				update ttkd_bsc.ct_bsc_ptm a 
				    set phanloai_kh = (case when exists(select 1 from ttkd_bct.dbkh_db 
																						where nhom in ('COOP','MAILINH') and regexp_replace(mst, '\D', '') = regexp_replace(a.mst, '\D', '') ) 
																			then 'DA2'  -- dbkh_db bang chi Nguyen
															 when loaitb_id=271 then 'DC2'
															 when ma_kh='GTGT rieng' 
																			  then (select (select ma_plkh from css_hcm.phanloai_kh where phanloaikh_id=b.plkh_id) 
																					    from ttkd_bct.db_thuebao_ttkd b 
																					    where to_number(regexp_replace(b.mst, '\D', '')) = to_number(regexp_replace(a.mst, '\D', ''))  and rownum=1)   
															when loaitb_id in (20) 
																  then (select (select ma_plkh from css_hcm.phanloai_kh where phanloaikh_id=b.plkh_id) 
																		    from ttkd_bct.db_thuebao_ttkd b 
																		    where loaitb_id in (20) and b.loaitb_id = a.loaitb_id and b.ma_tb = a.ma_tb and to_char(ngay_sd,'dd/mm/yyyy')=to_char(a.ngay_bbbg,'dd/mm/yyyy')
																		    )
															else (select phanloai_kh from hocnq_ttkd.temp_plkh where khachhang_id=a.khachhang_id)
												end)
--												, ma_kh = 'GTGT rieng'
--				     select thang_luong, thang_ptm, loaitb_id, dich_vu, ma_tb, ngay_bbbg, nguon, thuebao_id, ma_kh, ma_duan_banhang, chuquan_id, mst, khhh_khm, phanloai_kh, doituong_kh, khachhang_id from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm = 202502
								and chuquan_id in (145,264,266) and loaitb_id<>21 and phanloai_kh is null
								and nvl(thang_tldg_dt, 999999) >= 202502
					
				    ;
			
                   
				update ttkd_bsc.ct_bsc_ptm a
				    set heso_khachhang = case when heso_kvdacthu < 1 then 1                                                       -- ap dung cho thue bao thuoc kv doc quyen, Mai linh, Coop, vnpts Hoa Binh
																	when vanban_id = 764 then 1		--ap dung den thang 12/2024
																	when vanban_id = 1113868 then 1		--ap dung den thang 12/2024
																	when loaitb_id in (208) or
																				loaitb_id in (select loaitb_id from css_hcm.loaihinh_tb where upper(loaihinh_tb) like 'VNEDU%') then 1 --(mail anh Nghia trong folder xuly T11/2024, LDTT duyet)
																   when khhh_khm = 'KHM' then 1		---KH moi thi khong xet phan loai KH vi, xet phanloai_kh 6T/lan
																   when phanloai_kh in ('DA1','DA2','DB1') then 0.7
																	when phanloai_kh in ('DB2') then 0.85
																	when phanloai_kh in ('DB3','DC1','DC2') then 1
																	else 1 end
--								, thang_luong = 24
--				 select thang_luong, mst, heso_khachhang, heso_kvdacthu, doituong_kh, khhh_khm, phanloai_kh, nguon, thuebao_id, ma_duan_banhang, khachhang_id, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a
				    where a.thang_ptm = 202502 and nvl(thang_tldg_dt, 999999) >= a.thang_ptm
							and (loaitb_id<>21 or ma_kh='GTGT rieng') --and heso_khachhang is null
						
							
--							and thang_luong= 86
				    ;
--				select * from ttkd_bct.db_thuebao_ttkd where ma_tb = '84845911095';
--				select * from css_hcm.phanloai_kh;
          commit;
		rollback; 
        
-- He so thue bao ngan han: 
			update ttkd_bsc.ct_bsc_ptm a 
					set heso_tbnganhan = case when thoihan_id=2 and (loaitb_id in (58, 59, 39) or dichvuvt_id in (7, 8, 9)) then 0.3
																else 1 end
--			 	select heso_tbnganhan, dich_vu, thang_luong, nguon, ma_gd from ttkd_bsc.ct_bsc_ptm a
			    where a.thang_ptm = 202502--- thang n thang_luong in (87)
--							and thoihan_id=1 and dichvuvt_id in (1,10,11,4,7,8,9)
--							and thang_luong = 86
--							and heso_tbnganhan is null

			    ;
                           commit;
                       
-- He so dich vu DNHM+sodep:
				update ttkd_bsc.ct_bsc_ptm a 
					   set heso_dichvu_dnhm = (case --when chiphi_ttkd > 0 and thang_xn_chiphi_ttkd is not null then 0		---mail Chu Minh KHKT duyet chi phi TTKD (neu co)
																			when loaitb_id in (280, 292) then 0.3  -- phi tich hop
																		   when loaitb_id=35 then 0.4       -- VNPT iOffice-phi tich hop nhap o tien dnhm
																   else 0.1 end)
--				     select tien_dnhm, tien_sodep, heso_dichvu_dnhm, loaitb_id from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm = 202502 --- thang n
								and (loaitb_id<>21 or ma_kh='GTGT rieng')
								
				    ;
                          commit;          
        
-- he so ho tro:
				drop table hocnq_ttkd.temp_hesohotro purge;
				create table hocnq_ttkd.temp_hesohotro as 
						select hdtb_id from ttkd_bct.ptm_codinh_202502
										where nguoi_cn_goc in ('myvnpt','dhtt.mytv','ws_smes') and ma_gd_gt is not null
				;																			   
				update ttkd_bsc.ct_bsc_ptm a
				    set 
				    heso_hotro_nvptm  = case 
																	when a.loaitb_id = 20 and exists (select sdt_datmua, ma_gioithieu, tennv_gioithieu 
																																from ttkd_bsc.digishop 
																																where lower(trangthai_shop) like 'th_nh c_ng' and sdt_datmua = a.ma_tb and thang >= 202501) ---thang n- 1
																				then 0.5				---50% neu VNPts ban qua DIGISHOP
																	when a.dichvuvt_id not in (2, 14, 15, 16) 
																											and exists (select ma_dhsx, ma_gioithieu, tennv_gioithieu 
																																from ttkd_bsc.digishop 
																																where lower(trangthai_shop) like 'th_nh c_ng' and ma_dhsx = a.ma_gd_gt and thang >= 202501) ---thang n- 1
																				then 0.5			---50% neu dvu BR, CD ban qua DIGISHOP

																	when a.dichvuvt_id not in (2, 14, 15, 16) 
																											and exists (select ma_dhsxkd from khanhtdt_ttkd.IMP_SHOPCTV_DH
																																where VAITRO_CTV = 'CTV liên kết' and MA_DHSXKD is not null
																																				and a.ma_gd_gt  = ma_dhsxkd and thang >= 202501)			---thang n -1
																					then 0.5
																	   when ungdung_id in (4, 14, 17) and ma_gd_gt is not null then 0.5		---ban qua HD order tu App SmartCA/SDK SmartCA (ws_smes) BSUNG ungdung_id = 4, 17 test từ ngày 3/3/2025
																	   when exists (select 1 from hocnq_ttkd.temp_hesohotro
																							   where hdtb_id=a.hdtb_id)
																				then 0.5				---he thong khac tinh 50%

																	   when manv_hotro is not null and tyle_am is not null then tyle_am 		---AM 1 phan, PS ho tro du an
																	   when manv_ptm='VNP017772' and loaitb_id=288 
																				 and (hdtb_id, thuebao_id) in (select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL where thang = a.thang_ptm)
																							then 0.5   -- SmartCA tinh cho Huong BHOL 50%
																	  else 1 end
						,heso_hotro_nvhotro = case 
																		when a.loaitb_id = 20 and exists (select sdt_datmua, ma_gioithieu, tennv_gioithieu 
																																from ttkd_bsc.digishop 
																																where lower(trangthai_shop) like 'th_nh c_ng' and sdt_datmua = a.ma_tb and thang >= 202501) ---thang n- 1
																					then 0.5				---50% neu VNPts ban qua DIGISHOP
																		when a.dichvuvt_id not in (2, 14, 15, 16) 
																												and exists (select ma_dhsx, ma_gioithieu, tennv_gioithieu 
																																	from ttkd_bsc.digishop 
																																	where lower(trangthai_shop) like 'th_nh c_ng' and ma_dhsx = a.ma_gd_gt and thang >= 202501) ---thang n- 1
																					then 0.5			---50% neu dvu BR, CD ban qua DIGISHOP

																		when a.dichvuvt_id not in (2, 14, 15, 16) 
																											and exists (select ma_dhsxkd from khanhtdt_ttkd.IMP_SHOPCTV_DH
																																	where VAITRO_CTV = 'CTV liên kết' and MA_DHSXKD is not null
																																				and a.ma_gd_gt  = ma_dhsxkd and thang >= 202501)			---thang n -1
																					then 0.5
																					
																		when ungdung_id in (4, 14, 17) and ma_gd_gt is not null then 0.5		---ban qua HD order tu App SmartCA/SDK SmartCA (ws_smes) BSUNG ungdung_id = 4, 17 test từ ngày 3/3/2025
																		when exists (select 1 from hocnq_ttkd.temp_hesohotro
																							   where hdtb_id=a.hdtb_id)
																				then 0.5				---he thong khac tinh 50%
																		when manv_hotro is not null and tyle_hotro is not null then tyle_hotro		---PS ho tro du an
																		when manv_ptm='VNP017772' and loaitb_id=288 
																				 and (hdtb_id, thuebao_id) in (select hdtb_id, thuebao_id from ttkd_bsc.ptm_xuly_50_BHOL where thang = a.thang_ptm)
																							then 0.5   -- SmartCA tinh cho Huong BHOL 50%
																		when thang_luong = 71 and vanban_id = 764  then 0.8		--ap dung đen thang 202502 theo vanban_id, sau thang 7 bo 
																		else 0 end
						,
						heso_hotro_dai         = case when thang_luong = 71 and vanban_id = 764 then 0.2 else 0 end		--ap dung đen thang 202502 theo vanban_id, sau thang 7 bo  else 0 end
						--, thang_luong = 11
						
--		select thang_luong, thang_ptm, ma_tb, dich_vu, manv_hotro, tyle_hotro, tyle_am,  heso_hotro_nvptm, heso_hotro_nvhotro, heso_hotro_dai, ma_duan_banhang from ttkd_bsc.ct_bsc_ptm a    
				where (thang_ptm = 202502 --- thang n
								or thang_luong in (4, 44))			---flag 4 file so 6 sau khi update ma QLDA, so 5 update DIGISHOP, file Update anh Nghia thang_luong in (4, 44, 87)
					 and (loaitb_id<>21 or ma_kh='GTGT rieng') 
					 and nvl(thang_tldg_dt, 999999) >= 202502 
--					 and thang_luong = 71
--			and ma_duan_banhang = '254500'

				    ; 
                         commit;  
					rollback;


-- He so dia ban tinh khac: ngay 4/8/24 co sua dk ban CNTT tinh LAN va BDG
			update ttkd_bsc.ct_bsc_ptm 
				   set heso_diaban_tinhkhac = case when loaitb_id not in (20, 21, 149)
																					   and ( (dichvuvt_id in (1,10,11,12,4,7,8,9) and kieuld_id=13102)
																										or (dichvuvt_id in (13,14,15,16) and tinh_id not in (28))
																							 ) then 0.85
																			else 1 end
	---		select thang_luong, ma_gd, ma_tb, heso_diaban_tinhkhac, dichvuvt_id, kieuld_id, tinh_id, thuebao_id from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202502 --- thang n thang_luong in (87)
								 
--					and	 ma_tb = 'hcm_hddt_00017855'
--			and thang_luong = 86
					
				;
-----viet ham 
		---thang + 1 len *** quet hso nop tre so voi ngay 07 thang n+ 2, ap dung thang_ptm = n-3, n-2
		
---He so Ho so goc
		update ttkd_bsc.ct_bsc_ptm a 
				set  thang_luong = '3', heso_hoso = 0.8
--		     select thang_luong, ma_tb, tenkieu_ld, ngay_bbbg, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du, mien_hsgoc, bs_luukho , heso_hoso, thang_tldg_dt, luong_dongia_nvptm from ttkd_bsc.ct_bsc_ptm a
		    where ((thang_ptm = 202411 and (to_number(to_char(ngay_luuhs_ttkd,'yyyymmdd')) > 20250107 or to_number(to_char(ngay_luuhs_ttvt,'yyyymmdd')) > 20250107))	--thang n-3, ngay hso n-1
							or (thang_ptm = 202412 and (to_number(to_char(ngay_luuhs_ttkd,'yyyymmdd')) > 20250207 or to_number(to_char(ngay_luuhs_ttvt,'yyyymmdd')) > 20250207))	--thang n-2, ngay hso n
						)
					  and (loaitb_id not in (21) or ma_kh='GTGT rieng') and nvl(heso_hoso, 1) = 1
					  and mien_hsgoc is null and nop_du = 1 --and substr(bs_luukho,1,6)= '202502' --and heso_hoso is null		---thang n+1
					  and nvl(thang_tldg_dt, 999999) >= 202502  
--					  and thang_luong<>'3'
			  ;
           rollback;
		 commit;
-- Luu y kiem tra heso_diaban_tinhkhac cho GTGT rieng

        
/*-- Kiem tra:
select dich_vu, ma_tb, heso_diaban_tinhkhac, diachi_ld, tinh_id, ttvt_id from ttkd_bsc.ct_bsc_ptm
    where thang_ptm=202410 and (loaitb_id not in (20,21,149) or ma_kh='GTGT rieng' ) 
        and heso_diaban_tinhkhac is not null;             
*/
 

-- Kiem tra thue bao tinh doanh thu nhieu lan trong thang:
select ma_gd, ma_tb, nguon, loaihd_id, dthu_goi, kieuld_id,lydo_khongtinh_luong, thang_tldg_dt, lydo_khongtinh_dongia
    from ttkd_bsc.ct_bsc_ptm
    where thang_ptm = 202502 and loaitb_id<>21 and
                (ma_gd, ma_tb, loaitb_id) in 
                          (select ma_gd, ma_tb, loaitb_id from ttkd_bsc.ct_bsc_ptm 
                            where thang_ptm = 202502 and loaitb_id!=20 group by ma_gd, ma_tb, loaitb_id having count(*)>1
					   )
				;
                            
-- Vua tai lap dich vu (loaihd_id=7, ngung su dung qua 35 ngay) vua nang/ha cap : xet doanh thu tai lap dich vu theo goi moi (loaihd_id=7)
update ttkd_bsc.ct_bsc_ptm a 
		set lydo_khongtinh_luong=lydo_khongtinh_luong||'-Tinh luong theo hs tai lap'
--		    select * from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202502 and nguon like 'thaydoitocdo%' and lydo_khongtinh_luong not like '%Tinh luong theo hs tai lap%'
			   and exists (select 1 from ttkd_bsc.ct_bsc_ptm
							   where thang_ptm = a.thang_ptm and thuebao_id=a.thuebao_id and nguon like 'tailap%')
							   ;
		
		commit;


-- DTHU DON GIA 
			----858 vnd --> 800 vnd theo vb 322, eO 5696552_1488	--> TTKD
								---80% * 800 = 640 vnd theo vb 1485/TTKD HCM -NS 15/08/24 --> VTTP
			update ttkd_bsc.ct_bsc_ptm a 
						set dongia = 800, heso_hoso = 1
--			    select nguon, dongia, dichvuvt_id, thang_luong, thang_ptm, thang_tldg_dt, heso_hoso from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202502 --- thang n
					and (loaitb_id<>21 or ma_kh = 'GTGT rieng' ) 
--					and thang_luong = 86

			    ;                            
           commit;
           
-- cac dv ngoai tru VNPTT, SMS Brandname, Voice Brandnanme
			---88/TTr-?H-NS-KTKH ng�y ng�y 12/9/2022 
			--- Neu trong thang xuat hien Ma Dai ly moi so voi tap chot Dai ly thang truoc
					--- neu Dai ly nay do AM thuong (khac AM QLDL)  ho tro --> 5% * Dthu goi * heso_hotro_nvptm, va PS = 5% * Dthu goi * heso_hotro_nvhotro
					--- Neu AM QLDL hotro -> 0% DThu goi, va PS = 0% dthu goi
			--- Nguoc lai Neu Dai ly hien huu ton tai tap chot Daly ly thang truoc
					--- 0% AM, va PS tinh binh thuong nhu thue bao khong qua DAILY PTM
			-- Dthu dongia: ap dung heso AM, khong ap dung cac vi tri (ap dung tu PTM T8/2024)
			---Goi luong tinh chi tinh DTHU KPI và DON GIA DNHM theo vb 353
			update ttkd_bsc.ct_bsc_ptm a 
			    set doanhthu_dongia_nvptm   = case when heso_daily =  0.05 then round(dthu_goi * nvl(tyle_huongdt,1)* heso_daily * heso_hotro_nvptm, 0)
																			when heso_daily =  0 then 0		---AM QLDL, AM va Daily hien huu = 0
																			else	round(dthu_goi*nvl(tyle_huongdt,1) *heso_dichvu * nvl(heso_tratruoc,1)
																									* heso_quydinh_nvptm * heso_vtcv_nvptm * nvl(heso_kvdacthu,1)
																									* heso_hotro_nvptm * heso_khachhang * nvl(heso_tbnganhan,1)
																									* nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)
																								,0) end                                                                    
					,doanhthu_dongia_nvhotro = case when heso_daily =  0.05 then round(dthu_goi * nvl(tyle_huongdt,1)* heso_daily * heso_hotro_nvhotro, 0)
																			when heso_daily =  0 and exists (select 1  from ttkd_bsc.dm_daily_khdn 
																																		where thang = 202502 and thang_kyhd = a.thang_ptm and ma_daily = a.ma_nguoigt) ---Dai ly moi
																								then 0
																			when vanban_id = 764 then 0 --ap dung den thang 12/2024
																			else round(dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu * nvl(heso_tratruoc,1)
																								* heso_quydinh_nvhotro * heso_vtcv_nvhotro * nvl(heso_kvdacthu,1)
																								* heso_hotro_nvhotro * heso_khachhang * nvl(heso_tbnganhan,1)
																								* nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) 
																						,0) end
					,doanhthu_dongia_dai          = case when heso_daily =  0.05 then round(dthu_goi * nvl(tyle_huongdt,1)* heso_daily * heso_hotro_dai, 0)
																			when heso_daily =  0 and exists (select 1  from ttkd_bsc.dm_daily_khdn 
																																	where thang = 202502 and  thang_kyhd = a.thang_ptm and ma_daily = a.ma_nguoigt) ---Dai ly moi
																								then 0
																			when vanban_id = 764 then 0 --ap dung den thang 12/2024
																			else round(dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu * nvl(heso_tratruoc,1)
																																*heso_quydinh_dai * heso_vtcv_dai * nvl(heso_kvdacthu,1)
																																* heso_hotro_dai * heso_khachhang * nvl(heso_tbnganhan,1)
																																*nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)
																								, 0) end
--		    select heso_quydinh_nvhotro, nguon, sothang_dc, thang_luong, thang_ptm, ma_gd, ma_tb, dichvuvt_id, manv_hotro, heso_daily, dthu_goi, doanhthu_dongia_nvptm, doanhthu_dongia_nvhotro, doanhthu_dongia_dai, ma_kh, loaitb_id, thang_tldg_dt, lydo_khongtinh_luong from ttkd_bsc.ct_bsc_ptm a
			    where (thang_ptm = 202502 --- thang n
								or thang_luong in (1, 2, 3, 4, 44, 87)
								)			---flag 4 file so 6 sau khi update ma QLDA, file 1 ngan han, file 2 update tra truoc, file 3 hoso tre
							and (loaitb_id not in (21,131) or ma_kh='GTGT rieng')  --and dthu_goi >0
							and nvl(thang_tldg_dt, 999999)>=202502 
							and not (loaitb_id = 358 and thang_ptm = 202502) --- voice Brandname chua dc xet  tinh thang n 
--			   and ma_tb = '84916803831'
--			  and heso_daily is not null
--			and thang_luong = '3'

			    ;                       

            commit;
		  rollback;
-- Doanh thu goi tich hop: ap dung khong phan biet tap quan ly 
        -- tham khao bang huong dan cua anh Nghia tinh he so cac goi tich hop cho SMEs: VB 275/TTKD HCM-DH 22/06/2020:
		--vb 353
				update ttkd_bsc.ct_bsc_ptm a
				    set doanhthu_dongia_nvptm =
								    (case when goi_id=15599  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1) *nvl(heso_hoso,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.21/0.1434 ,0)  -- SME_NEW
											 when goi_id=15600  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1) *nvl(heso_hoso,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.25/0.17 ,0)  -- SME+
											 when goi_id=15602  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1) *nvl(heso_hoso,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.25/0.21 ,0)  -- SME_BASIC 1
											 when goi_id=15601  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1) *nvl(heso_hoso,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.35/0.30 ,0)  -- SME_BASIC 2   
											 when goi_id=15604  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1) *nvl(heso_hoso,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.19/0.13 ,0)  -- SME_SMART1
											 when goi_id=15603  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1) *nvl(heso_hoso,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.20/0.14 ,0)  -- SME_SMART2
											 when goi_id=15605  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1) *nvl(heso_hoso,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.20/0.16 ,0)  -- F_Pharmacy
											 when goi_id=15596  then round( (dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
																					   *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1) *nvl(heso_hoso,1)
																					   *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)  ) * 0.20/0.15 ,0)  -- F_ORM
									end)   
	---	    select doanhthu_dongia_nvptm, goi_id from ttkd_bsc.ct_bsc_ptm a
				    where (thang_ptm = 202502 --- thang n
								or thang_luong in (1, 2, 3, 4))			---flag 4 file so 5 import dung thu chuyen dung that
					and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605) 
					and nvl(thang_tldg_dt, 999999) >= 202502
					
				    ;
                      
                            
-- Luong don gia cac dv ngoai tru vnptt, SMS Brandname:
				update ttkd_bsc.ct_bsc_ptm a 
				    set luong_dongia_nvptm    = case when heso_daily = 0.05 then doanhthu_dongia_nvptm		----luong_dongia_AM  va PS = doanhthu_dongia_nvptm neu qua DAI LY
																			else round(nvl(doanhthu_dongia_nvptm,0)*dongia/1000 ,0) end
						,luong_dongia_nvhotro = case when heso_daily = 0.05 then doanhthu_dongia_nvhotro	----luong_dongia_AM  va PS = doanhthu_dongia_nvhotro neu qua DAI LY
																			else round(nvl(doanhthu_dongia_nvhotro,0)*dongia/1000 ,0) end
						,luong_dongia_dai         = 0--round(nvl(doanhthu_dongia_dai,0)*dongia/1000 ,0)
--		 select thang_luong, thang_ptm, ma_tb, doanhthu_dongia_nvptm, luong_dongia_nvptm, luong_dongia_nvhotro, luong_dongia_dai, thang_tldg_dt, vanban_id from ttkd_bsc.ct_bsc_ptm a 
				    where  (thang_ptm = 202502 --- thang n
								or thang_luong in (1, 2, 3, 4, 44, 87)
								)			----flag 4 file so 6 sau khi update ma QLDA, file 1 ngan han, file 2 update tra truoc, file 3 hoso tre
							and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') --and dthu_goi >0
							and nvl(thang_tldg_dt, 999999) >= 202502
							and not (loaitb_id = 358 and thang_ptm = 202502) --- voice Brandname chua dc xet tinh thang n 
				
                            ;
       commit;
-- SMS Brandname thang n-1: 
			update ttkd_bsc.ct_bsc_ptm a 
			    set doanhthu_dongia_nvptm = 
																	case when heso_daily =  0.05 then round((nvl(dthu_goi,0) + nvl(dthu_goi_ngoaimang,0)) * nvl(tyle_huongdt,1)* heso_daily * heso_hotro_nvptm, 0)
																			when heso_daily =  0 then 0		---AM QLDL, AM va Daily hien huu = 0
																			else	round( ( (nvl(dthu_goi,0)*heso_dichvu) +(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )																									
																									*nvl(tyle_huongdt,1) * nvl(heso_tratruoc,1)
																									* heso_quydinh_nvptm * heso_vtcv_nvptm * nvl(heso_kvdacthu,1)
																									* heso_hotro_nvptm * heso_khachhang * nvl(heso_tbnganhan,1)
																									* nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)
																								,0) end 
					,doanhthu_dongia_nvhotro = 
																	case when heso_daily =  0.05 then round((nvl(dthu_goi,0) + nvl(dthu_goi_ngoaimang,0)) * nvl(tyle_huongdt,1)* heso_daily * heso_hotro_nvhotro, 0)
																			when heso_daily =  0 and exists (select 1  from ttkd_bsc.dm_daily_khdn 
																																		where thang = 202502 and thang_kyhd = a.thang_ptm and ma_daily = a.ma_nguoigt) ---Dai ly moi
																								then 0
																			else round( ( (nvl(dthu_goi,0) * heso_dichvu) + (nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
																								* nvl(tyle_huongdt,1) * nvl(heso_tratruoc,1)
																								* heso_quydinh_nvhotro * heso_vtcv_nvhotro * nvl(heso_kvdacthu,1)
																								* heso_hotro_nvhotro * heso_khachhang * nvl(heso_tbnganhan,1)
																								* nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)
																						,0) end
					,doanhthu_dongia_dai = null
---	   select thang_luong, thang_ptm, heso_daily, heso_tratruoc, heso_quydinh_nvptm, heso_quydinh_nvhotro, heso_vtcv_nvptm, heso_hotro_nvptm, heso_khachhang, dthu_goi, dthu_goi_ngoaimang, doanhthu_dongia_nvptm  from ttkd_bsc.ct_bsc_ptm a
			    where (thang_ptm = 202501 --- thang n -1
								or thang_luong in (1, 2, 3, 4))			---flag 4 file so 5 import dung thu chuyen dung that
					and loaitb_id=131
						
			    ;
									    
			update ttkd_bsc.ct_bsc_ptm 
			    set luong_dongia_nvptm = case when heso_daily =  0.05 then doanhthu_dongia_nvptm
																	else nvl(doanhthu_dongia_nvptm,0)*dongia/1000 end
					 , luong_dongia_nvhotro = case when heso_daily =  0.05 then doanhthu_dongia_nvhotro
																	else nvl(doanhthu_dongia_nvhotro,0)*dongia/1000 end
					, luong_dongia_dai = 0
			    where (thang_ptm = 202501 --- thang n-1
								or thang_luong in (1, 2, 3, 4))			---flag 4 file so 5 import dung thu chuyen dung that
					and loaitb_id=131 
			    ;
                           
commit;
            
-- don gia dnhm: 
		---Goi Luong tinh: tinh don gia dnhm VNPts VB353, hok tính VB 344

			update ttkd_bsc.ct_bsc_ptm a 
			    set doanhthu_dongia_dnhm = case when heso_daily =  0.05 and heso_dichvu_dnhm > 0 then round((nvl(tien_dnhm,0)+nvl(tien_sodep,0)) * nvl(tyle_huongdt,1)* heso_daily, 0)
																		when heso_daily =  0 then 0
																			else round((nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *nvl(tyle_huongdt,1) *heso_dichvu_dnhm
																									 * heso_quydinh_nvptm * heso_vtcv_nvptm * nvl(heso_kvdacthu,1)
																									 * nvl(heso_diaban_tinhkhac,1)
																								,0) end
--		  select thang_luong, ma_tb, luong_dongia_nvptm, doanhthu_dongia_dnhm, tien_dnhm, tien_sodep, heso_daily, nguon, thang_tldg_dnhm from ttkd_bsc.ct_bsc_ptm a
			    where (thang_ptm = 202502 --- thang n
								or thang_luong in (1, 2, 3, 4)				--- thang_luong in (1, 2, 3, 4, 87)
								)			---flag 4 file so 5 import dung thu chuyen dung that
					
							   and (loaitb_id not in (21) or ma_kh='GTGT rieng') 
							   and (tien_dnhm>0 or tien_sodep>0) 
							   and nvl(thang_tldg_dnhm, 999999) >= 202502
							   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd 
												  where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id) 
						 --or loaitb_id=292)
					
						 ;  

                                                 
				update ttkd_bsc.ct_bsc_ptm a 
							set luong_dongia_dnhm_nvptm = case when heso_daily = 0.05 then doanhthu_dongia_dnhm
																							else round(nvl(doanhthu_dongia_dnhm,0) * dongia / 1000 ,0) end
--		  select thang_luong, ma_tb, luong_dongia_nvptm, doanhthu_dongia_dnhm, LUONG_DONGIA_DNHM_NVPTM, nguon, thang_tldg_dnhm from ttkd_bsc.ct_bsc_ptm a
				    where doanhthu_dongia_dnhm is not null           
							   and (thang_ptm = 202502 --- thang n
												or thang_luong in (1, 2, 3, 4)			-- thang_luong in (1, 2, 3, 4, 87)
												)			---flag 4 file so 5 import dung thu chuyen dung that
								and (loaitb_id not in (21) or ma_kh='GTGT rieng' ) 
								and nvl(thang_tldg_dnhm, 999999) >= 202502
								
				;

 commit;
rollback;
-- DTHU KPI  
-- nhan vien:
		-- Dthu KPI: tinh du 100% khong ap dung heso trong/ngoai diaban theo quy d?nh, heso_quydinh_nvptm, heso_quydinh_nvhotro nay(ap dung PTM T8/2024)
		-- khong xet dk ghi nhan:
			-- hoso goc
		---Goi Luong tinh: chi tinh don gia, Dthu KPI DNHM theo vb 353
			
				update ttkd_bsc.ct_bsc_ptm a
				    set  doanhthu_kpi_nvptm = case 
																			when heso_daily =  0.05 then doanhthu_dongia_nvptm
--			huy từ ky luong tháng 2							when heso_daily =  0 then 0		---AM QLDL, AM va Daily hien huu = 0
																			when vanban_id = 764 then 0 --ap dung den thang 12/2024
																			when loaitb_id = 20 and goi_luongtinh is not null then 0
																			else	round(dthu_goi*nvl(tyle_huongdt,1) * heso_dichvu * nvl(heso_tratruoc,1)
																									* heso_vtcv_nvptm
																									* heso_hotro_nvptm * nvl(heso_tbnganhan,1)
																									* nvl(heso_diaban_tinhkhac,1)
																								,0) end
						 , doanhthu_kpi_nvdai   = 
																	case 
																			when heso_daily =  0.05 then doanhthu_dongia_dai
																			when heso_daily =  0 and exists (select 1  from ttkd_bsc.dm_daily_khdn 
																																		where thang = 202502 and thang_kyhd = a.thang_ptm and ma_daily = a.ma_nguoigt) ---Dai ly moi
																								then 0
																			when loaitb_id = 20 and goi_luongtinh is not null then 0
																			else round(dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu * nvl(heso_tratruoc,1)
																								* heso_vtcv_dai
																								* heso_hotro_dai * nvl(heso_tbnganhan,1)
																								* nvl(heso_diaban_tinhkhac,1) 
																						,0) end
						 , doanhthu_kpi_nvhotro = case 
																			when heso_daily =  0.05 then doanhthu_dongia_nvhotro
																			when heso_daily =  0 and exists (select 1  from ttkd_bsc.dm_daily_khdn 
																																		where thang = 202502 and thang_kyhd = a.thang_ptm and ma_daily = a.ma_nguoigt) ---Dai ly moi
																								then 0
																			when loaitb_id = 20 and goi_luongtinh is not null then 0
																			else round(dthu_goi*nvl(tyle_huongdt,1)*heso_dichvu * nvl(heso_tratruoc,1)
																								* heso_vtcv_nvhotro
																								* heso_hotro_nvhotro * nvl(heso_tbnganhan,1)
																								* nvl(heso_diaban_tinhkhac,1) 
																						,0) end
						 , doanhthu_kpi_dnhm    = case 
																				when heso_daily =  0.05 and heso_dichvu_dnhm > 0 then doanhthu_dongia_dnhm
--			huy từ ky luong tháng 2								when heso_daily =  0 then 0
																				when loaitb_id = 20 and goi_luongtinh is not null then tien_dnhm
																					else round((nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *nvl(tyle_huongdt,1) *heso_dichvu_dnhm
																											 * heso_vtcv_nvptm
																											 * nvl(heso_diaban_tinhkhac,1)
																										,0) end
						
--		 select ma_tb, heso_quydinh_nvptm, heso_quydinh_nvhotro, heso_quydinh_dai, thang_luong, thang_ptm, doanhthu_dongia_nvptm, doanhthu_dongia_nvhotro, doanhthu_kpi_nvptm, doanhthu_kpi_nvdai, doanhthu_kpi_nvhotro, doanhthu_kpi_dnhm, thang_tlkpi from ttkd_bsc.ct_bsc_ptm a
				    where (thang_ptm = 202502 --- thang n
								or thang_luong in (1, 2, 3, 4, 87)
								)			---flag 4 file so 5 import dung thu chuyen dung that
									and dich_vu not like 'Thi_t b_ gi_i ph_p%' and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') 
									and nvl(thang_tlkpi, 999999) >= 202502	
					
				    ;


-- dtkpi cua dich vu Thiet bi giai phap:  la dthu hop dong x cac he so tinh bsc (ko tinh tren chenh lech thu chi).
					update ttkd_bsc.ct_bsc_ptm a
						   set doanhthu_kpi_nvptm  = case when heso_daily =  0.05
																									then round(dthu_goi_goc * nvl(tyle_huongdt,1) * heso_daily * heso_hotro_nvptm ,0)
--			huy từ ky luong tháng 2								when heso_daily =  0 then 0
																					else round(dthu_goi_goc * nvl(tyle_huongdt,1) * heso_dichvu * nvl(heso_tratruoc,1)
																														* heso_vtcv_nvptm
																														* heso_hotro_nvptm * nvl(heso_tbnganhan,1)
																														* nvl(heso_diaban_tinhkhac,1), 0)
																							end
							   ,doanhthu_kpi_nvhotro = case when heso_daily =  0.05 
																									then round(dthu_goi_goc * nvl(tyle_huongdt,1) * heso_daily * heso_hotro_nvhotro ,0)
																				   when heso_daily =  0 and exists (select 1  from ttkd_bsc.dm_daily_khdn 
																																		where thang = 202502 and thang_kyhd = a.thang_ptm and ma_daily = a.ma_nguoigt) ---Dai ly moi
																								then 0
																				   else round(dthu_goi_goc * nvl(tyle_huongdt,1)* heso_dichvu * nvl(heso_tratruoc,1)
																										* heso_vtcv_nvhotro
																										* heso_hotro_nvhotro * nvl(heso_tbnganhan,1)
																										* nvl(heso_diaban_tinhkhac,1), 0)
																					end         
--				select heso_quydinh_nvptm, heso_quydinh_nvhotro, heso_quydinh_dai, thang_luong, ma_duan_banhang, nguon, dthu_goi_goc, dthu_goi, doanhthu_kpi_nvptm, doanhthu_kpi_nvhotro, heso_khachhang from 	ttkd_bsc.ct_bsc_ptm a
					    where (thang_ptm = 202502 --- thang n
											or thang_luong in (1, 2, 3, 4, 44, 87))			---flag 4 file so 5 import dung thu chuyen dung that
									 and dich_vu like 'Thi_t b_ gi_i ph_p%'  
									
					    ; 
-- SMS Brandname thang n-1:
			update ttkd_bsc.ct_bsc_ptm a
			    set  doanhthu_kpi_nvptm = 
																	case when heso_daily =  0.05 then doanhthu_dongia_nvptm
--			huy từ ky luong tháng 2							when heso_daily =  0 then 0		---Daily moi va Daily hien huu = 0
																			else	round( ( (nvl(dthu_goi,0)*heso_dichvu) +(nvl(dthu_goi_ngoaimang,0) * heso_dichvu_1) )																									
																									*nvl(tyle_huongdt,1) * nvl(heso_tratruoc,1)
																									* heso_vtcv_nvptm
																									* heso_hotro_nvptm * nvl(heso_tbnganhan,1)
																									* nvl(heso_diaban_tinhkhac,1)
																								,0) end
					  , doanhthu_kpi_nvhotro = case when heso_daily =  0.05 then doanhthu_dongia_nvhotro
																			when heso_daily =  0 and exists (select 1  from ttkd_bsc.dm_daily_khdn 
																																		where thang = 202502 and thang_kyhd = a.thang_ptm and ma_daily = a.ma_nguoigt) ---Dai ly moi
																								then 0
																			else round( ( (nvl(dthu_goi,0) * heso_dichvu) + (nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
																								* nvl(tyle_huongdt,1) * nvl(heso_tratruoc,1)
																								* heso_vtcv_nvhotro
																								* heso_hotro_nvhotro * nvl(heso_tbnganhan,1)
																								* nvl(heso_diaban_tinhkhac,1)
																						,0) end
--			    select heso_quydinh_nvptm, heso_quydinh_nvhotro, heso_quydinh_dai, doanhthu_dongia_nvptm, doanhthu_kpi_nvptm, doanhthu_dongia_nvhotro, doanhthu_kpi_nvhotro, ma_tb from ttkd_bsc.ct_bsc_ptm
			    where (thang_ptm = 202501 --- thang n-1
								or thang_luong in (1, 2, 3, 4))			---flag 4 file so 5 import dung thu chuyen dung that
							 and loaitb_id = 131 
							 
			    ;
                            
            
-- DTHU KPI PHONG
		--Khong ap dung heso_quydinh_nvptm, ghi nhan 100%, khong ap dung 5% heso_daily
		
				update ttkd_bsc.ct_bsc_ptm a
					   set doanhthu_kpi_phong = (case when thang_luong = 86 and loaitb_id in (153, 40) and kieuld_id in (550, 13177, 13266) then 0 ----khong ting KPI khi gia han Int Truc tiep va Smart cloud
																				when dich_vu like 'Thi_t b_ gi_i ph_p%' 
																		  then round(dthu_goi_goc * nvl(tyle_huongdt,1) * heso_dichvu * nvl(heso_tratruoc,1)
																									   --* heso_vtcv_nvptm --* heso_hotro_nvptm
																									   * nvl(heso_tbnganhan,1)
																									   * nvl(heso_diaban_tinhkhac,1) ,0)
																	  else 
																			round( dthu_goi * nvl(tyle_huongdt,1) * heso_dichvu * nvl(heso_tratruoc,1)
																							   --* heso_vtcv_nvptm --* heso_hotro_nvptm
																							   * nvl(heso_tbnganhan,1)
																							   * nvl(heso_diaban_tinhkhac,1) ,0)
																		end)
--		   select heso_quydinh_nvptm, heso_quydinh_nvhotro, heso_quydinh_dai, thang_luong, heso_khachhang, heso_kvdacthu, doanhthu_kpi_phong, ma_tb, thang_tlkpi_phong from ttkd_bsc.ct_bsc_ptm a
				    where (thang_ptm = 202502 --- thang n
								or thang_luong in (1, 2, 3, 4, 87) 
								)			---flag 4 file so 5 import dung thu chuyen dung that
								and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') 
								and nvl(THANG_TLKPI_PHONG, 999999) >= 202502 
				    ;
                      commit;


 --  SMS Brandname thang n-1
				update ttkd_bsc.ct_bsc_ptm
				    set doanhthu_kpi_phong = round(((nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  
																		)
																    * nvl(tyle_huongdt,1)
																    * nvl(heso_tratruoc,1)
																    * heso_vtcv_nvptm --* heso_hotro_nvptm
																    * nvl(heso_tbnganhan,1)
																    * nvl(heso_diaban_tinhkhac,1) ,0)
--				     select dthu_goi, dthu_goi_ngoaimang, doanhthu_kpi_nvptm, doanhthu_kpi_phong from ttkd_bsc.ct_bsc_ptm
				    where (thang_ptm = 202501 --- thang n-1
											or thang_luong in (1, 2, 3, 4, 87))			---flag 4 file so 5 import dung thu chuyen dung that
								and loaitb_id=131 
				    ;
                            
            commit;
-- Kpi dnhm phong:
			--khong ap dung heso_daily, khong ap dung heso_quydinh_nvptm
			---Goi Luong tinh: khong tinh don gia VNPts, chi tinh don gia trong VNPtt
		update ttkd_bsc.ct_bsc_ptm a
		    set doanhthu_kpi_dnhm_phong = case when thang_luong = 86 and loaitb_id in (153, 40) and kieuld_id in (550, 13177, 13266) then 0 ----khong ting KPI khi gia han Int Truc tiep va Smart cloud
																			when loaitb_id=20 and goi_luongtinh is not null then tien_dnhm
																	else round( (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *nvl(tyle_huongdt,1) * heso_dichvu_dnhm
																					* heso_vtcv_nvptm --* heso_hotro_nvptm
																					* nvl(heso_diaban_tinhkhac,1) ,0)
																	end
	---	select thang_luong, doanhthu_kpi_dnhm_phong, heso_dichvu_dnhm from ttkd_bsc.ct_bsc_ptm a
		    where (thang_ptm = 202502 --- thang n
								or thang_luong in (1, 2, 3, 4))			---flag 4 file so 5 import dung thu chuyen dung that thang_luong in (1, 2, 3, 4, 87)
					and (loaitb_id not in (21) or ma_kh='GTGT rieng')
					   and (tien_dnhm>0 or tien_sodep>0)
					   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
			
					   ;
                            


--- DTHU KPI TO:
		---DN1 co to QLDL, Dai ly CNT ghi nhan KPI TO, nguoc lai dai ly khong
		---DN2, 3 khong co to QLDL, ghi nhan KPI TO
				update ttkd_bsc.ct_bsc_ptm a
				    set doanhthu_kpi_to =  doanhthu_kpi_phong
	---	select thang_luong, MA_NGUOIGT, doanhthu_kpi_to, thang_tlkpi_to from ttkd_bsc.ct_bsc_ptm a
				    where (thang_ptm = 202502 --- thang n
									or thang_luong in (1, 2, 3, 4, 87))			---flag 4 file so 5 import dung thu chuyen dung that
								and (loaitb_id !=21 or ma_kh='GTGT rieng')  
--							and thang_luong in (3) and vanban_id = 783670
				    ;
                
		commit;
		rollback;
		---clear du lieu theo vb vanban_id NSG khong ap dung tu thang 10
			
		update ttkd_bsc.ct_bsc_ptm a 
					set doanhthu_kpi_nvptm = 0		--nvptm khong KPI
							, DOANHTHU_DONGIA_NVHOTRO = 0  --nvhotro khong DONGIA
							, LUONG_DONGIA_NVHOTRO = 0				--nvhotro khong DONGIA
							, DOANHTHU_DONGIA_DAI = 0		--nvdai khong DONGIA
							, LUONG_DONGIA_DAI = 0					--nvdai khong DONGIA
--		select * from ttkd_bsc.ct_bsc_ptm a 
		where  vanban_id = 764 and thang_ptm = 202502 ---only 3 thang (T7, T8, T9) xoa
		;
-- ======

-- Kiem tra:
-- Cac thue bao chua co doanhthu_dongia_nvptm:
select thang_ptm, chuquan_id, lydo_khongtinh_luong,nguon,dich_vu, dichvuvt_id,loaitb_id, hdtb_id, thuebao_id,doituong_id,
            ma_gd, ma_tb , ngay_bbbg,thoihan_id,
            manv_ptm, tennv_ptm, ten_pb, 
            goi_id, datcoc_csd, dthu_goi_goc,dthu_goi,dthu_ps,dthu_ps_n1,heso_dichvu, heso_tratruoc,heso_quydinh_nvptm,heso_vtcv_nvptm,heso_hotro_nvptm  ,
            doanhthu_dongia_nvptm, luong_dongia_nvptm, trangthai_tt_id
from ttkd_bsc.ct_bsc_ptm a
where thang_ptm = 202501
            and loaitb_id not in (21,210) and ma_kh<>'GTGT rieng' 
            and lydo_khongtinh_luong is null and manv_ptm is not null and dthu_ps is not null
            and doanhthu_dongia_nvptm is null
            and loaitb_id not in (20,61,131,222,224,358) ; -- mytv home combo, sms brn

-- nv ho tro chua duoc tinh doanhthu_kpi_nvhotro:
select thang_luong, thang_ptm,dich_vu, dichvuvt_id, loaitb_id, tien_dnhm, tocdo_id, goi_id, dthu_ps, thuebao_id,ma_tb,dthu_goi,doanhthu_kpi_nvhotro , chuquan_id, heso_hotro_nvhotro
    from ttkd_bsc.ct_bsc_ptm
    where thang_ptm=202501 and (loaitb_id<>21 or ma_kh='GTGT rieng') 
    and manv_hotro is not null and dthu_ps is not null and doanhthu_kpi_nvhotro is null
    ;

-- dnhm:
select dichvuvt_id, loaitb_id, ma_tb, tien_dnhm, tien_sodep, (nvl(tien_dnhm,0)+nvl(tien_sodep,0)),heso_dichvu_dnhm, heso_quydinh_nvptm, nvl(heso_kvdacthu,1), heso_vtcv_nvptm, heso_daily,
            doanhthu_dongia_dnhm,  luong_dongia_dnhm_nvptm, doanhthu_kpi_dnhm, ghi_chu
from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm=202501 and (loaitb_id!=21 or ma_kh<>'GTGT rieng')
                                  and (tien_dnhm>0 or tien_sodep>0)
                                  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id );
             
           
-- SMS Brn:
select ma_tb, dthu_ps, dthu_ps_n1, dthu_goi, dthu_goi_ngoaimang, heso_dichvu, heso_dichvu_1,
            doanhthu_dongia_nvptm, doanhthu_kpi_nvptm, luong_dongia_nvptm, doanhthu_kpi_nvhotro, thang_tlkpi_hotro
    from ttkd_bsc.ct_bsc_ptm
    where thang_ptm = 202410 and loaitb_id=131;

-- TB ngan han:
select thang_ptm, ma_pb,manv_ptm,(select ten_nv from ttkd_bsc.nhanvien_202501 where manv_hrm=a.manv_ptm) ten_nv,
            ma_tb, ten_tb,dich_vu, ngay_bbbg,ngay_cat, trangthaitb_id,songay_sd, heso_tbnganhan,tien_dnhm,tien_tt, ngay_tt, soseri, dthu_goi, dthu_ps,
            doanhthu_dongia_nvptm, luong_dongia_nvptm,  doanhthu_kpi_nvptm, 
            luong_dongia_dnhm_nvptm, thang_tldg_dnhm
    from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm = 202501 and thoihan_id=1
            and dthu_goi<>dthu_ps;
   




