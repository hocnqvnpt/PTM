---ngay 4/5 anh Nghia chua duyet xong, doi khi nào xong moi chay
		select * from ttkd_bct.ptm_gtgt where thang=202404;
		delete from ttkd_bct.ptm_gtgt where thang=202404
;
		insert into ttkd_bct.ptm_gtgt
			    ( thang , id, ma_duan_banhang, ngaycn_nvptm, ngaycn_ntl, dich_vu, dichvuvt_id, loaitb_id, ma_gd, ma_tb, sohopdong, 
				  ten_kh, diachi_kh, mst,so_gt, ngay_nghiemthu, dthu_theohopdong, dthu_doitac, tendoitac, 
				  tien_tt, ngay_tt, sohoadon, tien_tt_khkt, ngay_tt_khkt, sohoadon_khkt, ngaycapnhat_khkt, chuquan_id, pbh_ptm_id, 
				  manv_ptm, tennv_ptm, ma_vtcv, ma_to, ten_to, ma_pb, ten_pb, loai_ld, manv_hotro, tyle_hotro, tyle_hotro_nv, tyle_am, 
				  dthu_goi_goc, dthu_goi , dthu_ps, heso_dichvu, nguon, ghi_chu);
				  
			    select 202404 thang, d.id, d.ma_yeucau, d.ngaycapnhat, d.ngaycapnhat_tl, b.loaihinh_tb dich_vu, b.dichvuvt_id, b.loaitb_id_obss, d.ma_gd,  
							   d.ma_tb, d.sohopdong, upper(a.ten_khachhang) ten_kh, substr(trim(so_nha)||', '||nvl(ten_duong,'')||', '||nvl(ten_phuong,'')||', '||nvl(a.ten_quan,'') ,1,500) diachi_kh,
							   a.masothue mst, a.masothue so_gt, d.ngaynghiemthu, d.dt_hopdong, d.dt_doitac, trim(substr(d.tendoitac,1,500)) tendoitac,
							   d.sotienthu, d.ngaythanhtoan, d.sohoadon, d.tienthu_khkt tien_tt_khkt, d.ngaythanhtoan_khkt, d.sohoadon_khkt, d.ngaycapnhat_khkt,  
							   145 chuquan_id, (select pb.pbh_id from ttkd_bsc.dm_phongban pb where pb.ma_pb=e.ma_pb and pb.active=1) pbh_ptm_id,
							   d.manv_am manv_ptm, e.ten_nv tennv_ptm, e.ma_vtcv, e.ma_to, e.ten_to, e.ma_pb, e.ten_pb, e.loai_ld,
							   c.manv_presale_hrm manv_hotro, c.tyle_nhom/100 tyle_hotro, c.tyle_nhom/100 tyle_hotro_nv, decode(tyle_am,0,1,c.tyle_am/100) tyle_am, 
							   dt_goi_bsc dthu_goi_goc, dt_goi_dongia  dthu_goi, dt_goi_bsc dthu_ps, d.hesodichvu heso_dichvu, 'web123-ID447', d.ghichu_ntl
			
			    from ttkdhcm_ktnv.amas_yeucau a, ttkdhcm_ktnv.amas_loaihinh_tb b
							    , (select c1.*, f1.ma_dichvu from ttkdhcm_ktnv.amas_booking_presale c1, ttkdhcm_ktnv.amas_yeucau_dichvu f1 
								 where c1.ps_truong=1 and c1.ma_yeucau=f1.ma_yeucau and c1.id_ycdv = f1.id_ycdv
								 ) c
							    , (select * from ttkdhcm_ktnv.amas_duan_ngoai_doanhthu ptm
								 where trangthai_tl=10 and dexuat=7 and ma_dichvu not in (20,149) --and id=4946446
									    and trunc(ngaycapnhat) <= to_date('30/04/2024','dd/mm/yyyy') 			---ngay 31 thang n
									    and trunc(ngaycapnhat_tl) <= to_date('08/05/2024','dd/mm/yyyy') 			---ngay 8 thang n+ 1
									    and not exists(select 1 from ttkd_bct.ptm_gtgt where id=ptm.id)                 
								 )d
							    , ttkd_bsc.nhanvien_202404 e
							    --, css_hcm.hd_thuebao hd
			    where d.ma_yeucau=a.ma_yeucau and d.ma_dichvu=b.loaitb_id  
				   and d.ma_yeucau=c.ma_yeucau(+) and b.loaitb_id=c.ma_dichvu(+) 
				   and d.manv_am=e.ma_nv(+) 
			    order by id
			    ;
    
    commit;
select * from ttkd_bct.ptm_gtgt where thang=202404;  

/*     
select * from ttkd_bct.ptm_gtgt
    where (loaitb_id, ten_kh) in (select loaitb_id, ten_kh from ttkd_bct.ptm_gtgt where thang=202403 group by loaitb_id, ten_kh having count(*)>1) 
    order by loaitb_id, ten_kh;

trangthai_tl --> 9: chua duyet tính luong; 10: da duyet tinh luong
select * from ttkdhcm_ktnv.amas_yeucau_dichvu where id_ycdv in (1214603);
select * from ttkdhcm_ktnv.amas_booking_presale where ma_yeucau=165465;
select * from ttkdhcm_ktnv.amas_duan_ngoai_doanhthu d 
    where ma_yeucau in (120816,
126015,
95547,
142548,
147688,
127615,
128447,
143572,
133275,
95548,
162565,
225592,
225595,
189175);
select * from ttkdhcm_ktnv.amas_yeucau where ma_yeucau=120721;
select * from ttkdhcm_ktnv.amas_option;
*/