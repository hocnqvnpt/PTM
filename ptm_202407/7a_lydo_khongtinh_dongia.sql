update ttkd_bsc.ct_bsc_ptm a set lydo_khongtinh_dongia = null
--		   select thang_luong, thang_ptm, ma_tb, lydo_khongtinh_dongia, thang_tldg_dt, thang_tldg_dt_nvhotro, thang_tlkpi_phong from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm>=202404 and lydo_khongtinh_dongia is not null---thang n-3
						and (nvl(thang_tldg_dt, 999999) >= 202407 or nvl(thang_tlkpi_phong, 999999) >=202407)
					--	and thang_tldg_dt = 202406 or (thang_tlkpi_phong =202406 and heso_daily is not null)
					--and ma_tb = 'hutn1077515'
		   
    ;    
commit;
rollback;

   -- Thue bao khong tinh luong:
		update ttkd_bsc.ct_bsc_ptm a 
			   set lydo_khongtinh_dongia = lydo_khongtinh_luong
--		    select lydo_khongtinh_dongia, lydo_khongtinh_luong, thang_tldg_dt, thang_tlkpi_phong from  ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm >= 202404 -------thang n-3
					and (upper(loai_ld) not like '_LCN' or loai_ld is null)
					and lydo_khongtinh_luong is not null and lydo_khongtinh_luong<>'X' and lydo_khongtinh_dongia is null
				
					and (thang_tldg_dt is null or thang_tlkpi_phong is null)
					--  and ma_tb = 'hcm_ca_00097176'
					  ;
	----Dai ly ca nhan --> a Nguyen tinh
		update ttkd_bsc.ct_bsc_ptm a 
			   set lydo_khongtinh_dongia = lydo_khongtinh_luong
--		    select lydo_khongtinh_dongia, lydo_khongtinh_luong, thang_tldg_dt, thang_tlkpi_phong from  ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm >= 202404 -------thang n-3
					and upper(loai_ld)  like '_LCN' 
					and lydo_khongtinh_dongia is null
					and (thang_tldg_dt is null or thang_tlkpi_phong is null)
		;
-- No cuoc ps dau tien:    
				update ttkd_bsc.ct_bsc_ptm a 
							set lydo_khongtinh_dongia = lydo_khongtinh_dongia||';No cuoc'
--				select thang_ptm, nocuoc_ptm from ttkd_bsc.ct_bsc_ptm a 
				    where ma_kh<>'GTGT rieng' and loaitb_id<>21 and thang_tldg_dt is null 
								   and (lydo_khongtinh_dongia is null or lydo_khongtinh_dongia  not like '%No cuoc%')
								   and ((thang_ptm = 202407 and nocuoc_ptm is not null)           ---thang n     
											 or (thang_ptm = 202406 and nocuoc_n1 is not null)				---thang n-1
											 or (thang_ptm = 202405 and nocuoc_n2 is not null)				--thang n -2
											 or (thang_ptm = 202404 and nocuoc_n3 is not null))			---thang n-3
							 ;
				commit;                 


  -- Trang thai
			update ttkd_bsc.ct_bsc_ptm a 
					set lydo_khongtinh_dongia=lydo_khongtinh_dongia||';Trang thai tbao'
--			    select thang_luong, thang_ptm, ma_gd, ma_tb from ttkd_bsc.ct_bsc_ptm a 
			    where ma_kh<>'GTGT rieng' and loaitb_id not in (21,89,90,146) and thang_tldg_dt is null 
						 and (lydo_khongtinh_dongia not like '%Trang thai tbao%' or lydo_khongtinh_dongia is null)
						 and ( -- thang n:   
								   (a.thang_ptm = 202407
										   and not exists (select 1 from ttkd_bsc.ct_bsc_ptm 
																	where thang_ptm = a.thang_ptm and (trangthaitb_id=1 or (thoihan_id=1 and (dichvuvt_id in (8,9) or loaitb_id in (1,58,59,39) ) ) ) 
																		   and ma_tb=a.ma_tb and ma_kh=a.ma_kh and loaitb_id=a.loaitb_id and tenkieu_ld=a.tenkieu_ld)
														)
								   or
								   (a.thang_ptm = 202406
								   and not exists (select 1 from ttkd_bsc.ct_bsc_ptm 
															where thang_ptm = a.thang_ptm and ( trangthaitb_id_n1=1 or (thoihan_id=1 and (dichvuvt_id in (8,9) or loaitb_id in (1,58,59,39) ) ) )   
																   and ma_tb=a.ma_tb and ma_kh=a.ma_kh and loaitb_id=a.loaitb_id and tenkieu_ld=a.tenkieu_ld))
								   or 
								   (a.thang_ptm = 202405
								   and not exists (select 1 from ttkd_bsc.ct_bsc_ptm 
															where thang_ptm = a.thang_ptm and ( trangthaitb_id_n2=1 or (thoihan_id=1 and (dichvuvt_id in (8,9) or loaitb_id in (1,58,59,39) ) ) )   
																   and ma_tb=a.ma_tb and ma_kh=a.ma_kh and loaitb_id=a.loaitb_id and tenkieu_ld=a.tenkieu_ld))
								   or
								   (a.thang_ptm = 202404
								   and not exists (select 1 from ttkd_bsc.ct_bsc_ptm 
															where thang_ptm = a.thang_ptm and ( trangthaitb_id_n3=1 or (thoihan_id=1 and (dichvuvt_id in (8,9) or loaitb_id in (1,58,59,39) ) ) )   
																   and ma_tb=a.ma_tb and ma_kh=a.ma_kh and loaitb_id=a.loaitb_id and tenkieu_ld=a.tenkieu_ld))
							    );   
			  
			commit;


-- Ho so:
			update ttkd_bsc.ct_bsc_ptm a 
				set lydo_khongtinh_dongia=lydo_khongtinh_dongia||'; Chua nop du ho so' 
--			    select thang_luong, thang_ptm, ma_gd, ma_tb, nop_du, mien_hsgoc, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a 				
			    where thang_ptm>= 202404
								   and ma_kh<>'GTGT rieng' and loaitb_id<>21 and thang_tldg_dt is null 
								   and (nop_du=0 or nop_du is null) and mien_hsgoc is null
								   and (lydo_khongtinh_dongia not like '%so%' or lydo_khongtinh_dongia is null)
--								   and ma_tb = 'hcm_ivan_00021828'
				   ;
									
			commit
			;


		update ttkd_bsc.ct_bsc_ptm a 
					 set thang_tldg_dt=null, thang_tlkpi=null, thang_tlkpi_to=null, thang_tlkpi_phong=null,
						   lydo_khongtinh_dongia='ACT chua xac nhan tinh trang thanh toan'
--		     select * from ttkd_bsc.ct_bsc_ptm a 
		    where thang_ptm >= 202404
					 and (ma_kh='NewLife' or chuquan_id=264) 
					 and thang_xacnhan_khkt is null
					 and thang_tldg_dt is null
					 ;
			 
		commit;



-- Sms Brandname:
				update ttkd_bsc.ct_bsc_ptm 
						  set  thang_tldg_dt=null, thang_tlkpi=null, thang_tlkpi_to=null, thang_tlkpi_phong=null,
								lydo_khongtinh_dongia=lydo_khongtinh_dongia||'; Xet thang sau'  
--					     select thang_ptm, ma_tb, thang_tldg_dt, lydo_khongtinh_luong, lydo_khongtinh_dongia, nguon from ttkd_bsc.ct_bsc_ptm a               
				    where thang_ptm = 202407 		---thang n
								and loaitb_id=131 and lydo_khongtinh_luong is null ;
--							   and thang_tldg_dt is null
				commit;
rollback;

-- Voice Brandname: 
			update ttkd_bsc.ct_bsc_ptm a
				   set thang_tldg_dt=null, thang_tlkpi=null, thang_tlkpi_to=null, thang_tlkpi_phong=null,
						 lydo_khongtinh_dongia=lydo_khongtinh_dongia||'Sau thang phat trien, don vi gui hop dong cho anh Nghia xem xet'
--			     select thang_ptm, ma_tb, thang_tldg_dt, lydo_khongtinh_luong, lydo_khongtinh_dongia, nguon from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm >= 202404 and loaitb_id=358 and nvl(dthu_goi,0)=0
							   and lydo_khongtinh_dongia not like '%gui hop dong cho anh Nghia xem xet%'
							   and thang_tldg_dt is null
				   ;


-- Thue bao ngan han chua thanh ly:
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt=null, thang_tlkpi=null, thang_tlkpi_to=null, thang_tlkpi_phong=null
					,lydo_khongtinh_dongia= lydo_khongtinh_dongia ||';Thue bao ngan han nhung chua thanh ly hop dong'
--				select thang_ptm, ma_tb, thang_tldg_dt, lydo_khongtinh_luong, lydo_khongtinh_dongia, nguon from ttkd_bsc.ct_bsc_ptm a 
			    where thoihan_id=1 and loaitb_id not in (90,146) 
					  and not exists(select trangthaitb_id from css_hcm.db_thuebao where thuebao_id=a.thuebao_id and trangthaitb_id not in (7,9) )
					  and ((thang_ptm = 202404 and trangthaitb_id_n3 not in (7,9)) 
							    or (thang_ptm = 202405 and trangthaitb_id_n2 not in (7,9)) 
							    or (thang_ptm = 202406 and trangthaitb_id_n1 not in (7,9))
							    or (thang_ptm = 202407 and trangthaitb_id not in (7,9))
							)
--					  and (thang_tldg_dt = 202406 or thang_tldg_dt is null)
					  and nvl(thang_tldg_dt, 999999) >= 202407
					  and lydo_khongtinh_dongia not like '%Thue bao ngan han nhung chua thanh ly hop dong%'
					  ; 
commit;


-- Thue bao chua phat sinh cuoc
		--cac thue bao khong psc except 13 dvu tinh theo gia han tra truoc
		update ttkd_bsc.ct_bsc_ptm a 
				  set thang_tldg_dt         = case when thang_tldg_dt=202407 then null end
					   ,thang_tlkpi             = case when thang_tlkpi=202407 then null end
					   ,thang_tlkpi_to        = case when thang_tlkpi_to=202407 then null end
					   ,thang_tlkpi_phong = case when thang_tlkpi_phong=202407 then null end
					   ,thang_tldg_dnhm  = case when thang_tldg_dnhm=202407 then null end
					   ,thang_tlkpi_dnhm = case when thang_tlkpi_dnhm=202407 then null end
					   ,thang_tlkpi_dnhm_to = case when thang_tlkpi_dnhm_to=202407 then null end
					   ,thang_tlkpi_dnhm_phong = case when thang_tlkpi_dnhm_phong=202407 then null end
					   ,thang_tldg_dt_nvhotro = case when thang_tldg_dt_nvhotro=202407 then null end
					   ,thang_tlkpi_hotro = case when thang_tlkpi_hotro=202407 then null end
					   , thang_tldg_dt_dai = case when thang_tldg_dt_dai=202407 then null end
					   ,lydo_khongtinh_dongia = case when (lydo_khongtinh_dongia like '%chua phat sinh cuoc%' or lydo_khongtinh_dongia is null) then lydo_khongtinh_dongia || '; Thue bao chua phat sinh cuoc' end
-- 			select id, thang_luong, thang_ptm, ma_gd, ma_tb, loaitb_id, nguon, thang_tldg_dt, thang_bddc, lydo_khongtinh_luong, lydo_khongtinh_dongia, chuquan_id, dthu_goi from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm >= 202404 and (loaitb_id<>21 or ma_kh = 'GTGT rieng') 
			   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
			   and nvl(thang_tldg_dt, 999999) >= 202407
			   and lydo_khongtinh_dongia is null
			   and not (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)=0
										and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc >= a.thang_ptm
													)
			   and not (loaitb_id = 17 and thuebao_cha_id is not null)
			   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)=0
--			   and nvl(thang_bddc, 0) <= 202406		---thang n
--			   and ma_tb = 'quangtrc8931672'
			   ;

		commit;
		rollback;
-- Trang thai thanh toan hop dong
		update ttkd_bsc.ct_bsc_ptm a
		    set  thang_tldg_dt='', thang_tlkpi='', thang_tlkpi_to='', thang_tlkpi_phong=''
				  ,thang_tldg_dnhm='', thang_tlkpi_dnhm='', thang_tlkpi_dnhm_to='', thang_tlkpi_dnhm_phong=''
				  ,thang_tldg_dt_nvhotro='', thang_tlkpi_hotro='', thang_tldg_dt_dai = ''
				  ,lydo_khongtinh_dongia=lydo_khongtinh_dongia||';Trang thai hop dong chua thu tien'
--		     select thang_luong, thang_ptm, thang_tldg_dt, ma_gd, ma_tb, trangthai_tt_id, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm >= 202404 and chuquan_id<>264 and thang_tldg_dt is null and loaitb_id not in (20,21,149) 
				  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
				  and (trangthai_tt_id is null or trangthai_tt_id=0) 
				  and (lydo_khongtinh_dongia is null or lydo_khongtinh_dongia not like '%hop dong chua thu tien%')
				 -- and lydo_khongtinh_dongia is null ---check lai dieu kien nay co trung hay khong?
				  ;
				  
-- Khac chu quan HCM
		update ttkd_bsc.ct_bsc_ptm a
		    set  thang_tldg_dt='', thang_tlkpi='', thang_tlkpi_to='', thang_tlkpi_phong=''
				  ,thang_tldg_dnhm='', thang_tlkpi_dnhm='', thang_tlkpi_dnhm_to='', thang_tlkpi_dnhm_phong=''
				  ,thang_tldg_dt_nvhotro='', thang_tlkpi_hotro='', thang_tldg_dt_dai = ''
				  ,lydo_khongtinh_dongia=lydo_khongtinh_dongia||';Chu quan khong thuoc TTKDHCM'
--		     select thang_ptm, thang_tldg_dt, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm >= 202404 and thang_tldg_dt is null and loaitb_id not in (20,21,149) 
				  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
				  and (lydo_khongtinh_dongia is null or lydo_khongtinh_dongia not like '%Chu quan khong thuoc%')
--				  and lydo_khongtinh_dongia is null ---check lai dieu kien nay co trung hay khong?
				and chuquan_id not in  (145,266)
				  ;
				  
			----Thue bao 30B + D , cuoc ps only so chu, cac so con dc tinh can cu so chu
				---30B+D: cuoc PS cac thue bao con --> so chu thue bao cha, neu so chu no cuoc thi cac tbao con hok tinh
				-- thue bao con trang thai khong hoat dong binh thuong --> khong tinh
			update ttkd_bsc.ct_bsc_ptm a 
						set lydo_khongtinh_dongia = lydo_khongtinh_dongia || (select '; sochu: ' ||ma_tb|| lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm where thuebao_id = a.thuebao_cha_id)
--			select thang_ptm, ma_tb, thuebao_id, thuebao_cha_id, doanhthu_dongia_nvptm, thang_tldg_dt, lydo_khongtinh_dongia, (select 'sochu: ' ||ma_tb|| lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm where thuebao_id = a.thuebao_cha_id) lydo from ttkd_bsc.ct_bsc_ptm a
			where loaitb_id = 17 and thang_ptm >= 202404
							and thuebao_cha_id is not null and thang_tldg_dt is null
							and lydo_khongtinh_dongia not like '%Trang thai tbao%'
--							and ma_gd = 'HCM-LD/01613031'
;
            commit;
		  
		  ----Quyet tinh cac so con 30B+D theo so chu (thuebao_cha_id duoc tinh)
		
		update ttkd_bsc.ct_bsc_ptm a
				set thang_tldg_dt = (case 
																when exists (select 1 from ttkd_bsc.ct_bsc_ptm where thuebao_id = a.thuebao_cha_id and thang_tldg_dt is not null)
																		then (select thang_tldg_dt from ttkd_bsc.ct_bsc_ptm where thuebao_id = a.thuebao_cha_id and thang_tldg_dt is not null)
																else null
												end)
						, lydo_khongtinh_dongia = null
--		select ma_tb, thuebao_id, thuebao_cha_id, case 
--																when exists (select 1 from ttkd_bsc.ct_bsc_ptm where thuebao_id = a.thuebao_cha_id and thang_tldg_dt is not null)
--																		then (select thang_tldg_dt from ttkd_bsc.ct_bsc_ptm where thuebao_id = a.thuebao_cha_id and thang_tldg_dt is not null)
--																else null
--															end kq_thuebao_cha_id, thang_tldg_dt, lydo_khongtinh_dongia
--		from ttkd_bsc.ct_bsc_ptm a
		where (thuebao_cha_id is not null and loaitb_id = 17)		---bsung 22/6
					and thang_ptm >= 202404					---thang n-3
					and nvl(thang_tldg_dt, 999999) >= 202407			--thang n
					and lydo_khongtinh_dongia not like '%Trang thai tbao%'
--					and ma_gd in ('HCM-LD/01613023', 'HCM-LD/01613031')
;
-- Kiem tra thue bao chua cap nhat ly do khong tinh don gia:
select thang_ptm, trangthaitb_id, nocuoc_ptm,ten_pb, dich_vu, ma_gd, ma_tb, thuebao_id, thuebao_cha_id,
                manv_ptm, loai_ld, dthu_goi, luong_dongia_nvptm,lydo_khongtinh_luong, lydo_khongtinh_dongia, thoihan_id,ghi_chu,
                trangthaitb_id_n3, nocuoc_n3, ngay_luuhs_ttkd, ngay_luuhs_ttvt, ngay_scan,nop_du, mien_hsgoc,
                nvl(dthu_ps,0),nvl(dthu_ps_n1,0),nvl(dthu_ps_n2,0),nvl(dthu_ps_n3,0),
                thang_tldg_dt, trangthai_tt_id, nguon
        from ttkd_bsc.ct_bsc_ptm a           
        where thang_ptm=202406 and chuquan_id in (145,264,266)
                    and lydo_khongtinh_luong is null and loaitb_id<>21 and ma_kh<>'GTGT rieng'
                    and manv_ptm is not null and ma_vtcv is not null and loai_ld not like '_LCN'
                    and thang_tldg_dt is null 
                    and lydo_khongtinh_dongia is null;
                    
 
select thang_ptm, ten_pb, ten_to,dich_vu, ma_tb, thoihan_id,
                manv_ptm,loai_ld, thang_ptm,dthu_goi,lydo_khongtinh_luong, lydo_khongtinh_dongia,
                trangthaitb_id_n2, nocuoc_n2, ngay_luuhs_ttkd, ngay_luuhs_ttvt, ngay_scan,nop_du,
                dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3, trangthai_tt_id
        from ttkd_bsc.ct_bsc_ptm a           
        where thang_ptm=202403
                    and lydo_khongtinh_luong is null and loaitb_id<>21 and ma_kh<>'GTGT rieng'
                    and manv_ptm is not null and ma_vtcv is not null and loai_ld not like '_LCN'
                    and thang_tldg_dt is null 
                    and lydo_khongtinh_dongia is null;

select thang_luong, thang_ptm, ten_pb, dich_vu, id_447,  ma_tb, ma_gd, ma_kh,  thoihan_id,
                manv_ptm,loai_ld,dthu_goi,lydo_khongtinh_luong, lydo_khongtinh_dongia, 
                trangthaitb_id_n1, nocuoc_n1, ngay_luuhs_ttkd, ngay_luuhs_ttvt, ngay_scan,nop_du, thang_tlkpi,
                dthu_ps, dthu_ps_n1, dthu_ps_n2, dthu_ps_n3,trangthai_tt_id
        from ttkd_bsc.ct_bsc_ptm a           
        where thang_ptm=202404 
                    and lydo_khongtinh_luong is null and (loaitb_id<>21 or ma_kh='GTGT rieng')
                    and manv_ptm is not null and ma_vtcv is not null and loai_ld not like '_LCN'
                    and thang_tldg_dt is null
                    and lydo_khongtinh_dongia is null;
                    
select thang_ptm,ma_to, ma_gd, ma_tb, dich_vu, ten_tb, dthu_goi_goc, dthu_goi, ngay_bbbg, thoihan_id, a.ngay_luuhs_ttkd, 
                a.ngay_luuhs_ttvt, a.mien_hsgoc,a.nop_du,a.trangthaitb_id, dthu_ps, a.nocuoc_ptm
                lydo_khongtinh_luong, lydo_khongtinh_dongia, datcoc_csd, thang_bddc, trangthai_tt_id
        from ttkd_bsc.ct_bsc_ptm a
        where thang_ptm=202405 ---thang n
                    and lydo_khongtinh_luong is null and loaitb_id<>21 and ma_kh<>'GTGT rieng'
                    and manv_ptm is not null and ma_vtcv is not null and loai_ld not like '_LCN'
                    and thang_tldg_dt is null 
                    and lydo_khongtinh_dongia is null
				and dichvuvt_id not in (2)
				;

   
 -- Kiem tra thue bao ko tinh don gia nguyen nhan khac:
select thang_ptm,ma_to, ma_gd, ma_tb, dich_vu, ten_tb, dthu_goi_goc, dthu_goi, ngay_bbbg,a.ngay_luuhs_ttkd, 
                a.ngay_luuhs_ttvt, a.thoihan_id, a.nocuoc_ptm, a.mien_hsgoc,a.nop_du,a.trangthaitb_id, 
                lydo_khongtinh_luong, lydo_khongtinh_dongia
        from ttkd_bsc.ct_bsc_ptm a
        where thang_ptm>=202403
                    and lydo_khongtinh_luong is null and loaitb_id<>21 and ma_kh<>'GTGT rieng'
                    and manv_ptm is not null
                    and thang_tldg_dt is null 
                    and lydo_khongtinh_dongia not like '%ho so%'
                    and lydo_khongtinh_dongia not like '%thai%'
                    and lydo_khongtinh_dongia not like '%cuoc%'
                    and lydo_khongtinh_dongia not like '%Thue bao ngan han %'
                    and lydo_khongtinh_dongia not like '%NewLife - Chua xac nhan tinh trang thanh toan%'
                    and lydo_khongtinh_dongia not like '%NewLife chua xac nhan tinh trang thanh toan%'
                    and lydo_khongtinh_dongia not like '%ACT chua xac nhan tinh trang thanh toan%'
                    and lydo_khongtinh_dongia not like '%PKHKT xac nhan chua thu tien%'
                    and lydo_khongtinh_dongia not like '%Xet thang sau%'
                    and lydo_khongtinh_dongia not like '%Thue bao chua phat sinh cuoc%'
                    and lydo_khongtinh_dongia not like '%Dich vu khong tinh luong%'
                    and lydo_khongtinh_dongia not like '%ptm kenh CTVXHH%'; 
                    