package com.spinsys.mdaca.storage.explorer.milton;

import io.milton.annotations.Authenticate;
import io.milton.annotations.ChildOf;
import io.milton.annotations.ChildrenOf;
import io.milton.annotations.Get;
import io.milton.annotations.ResourceController;
import io.milton.annotations.Root;
import io.milton.annotations.Users;
import io.milton.common.ModelAndView;
import io.milton.http.http11.auth.DigestResponse;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

@ResourceController
public class HelloWorldController  {
 
    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(HelloWorldController.class);
 
    private List<Product> products = new ArrayList<Product>();
 
    public HelloWorldController() {
        products.add(new Product("hello"));
        products.add(new Product("world"));
        products.add(new Product("storage-explorer"));
    }
    
    @Authenticate
    public Boolean verifyPassword(DavUser m, String requestedPassword) {
        // calculate hash of requestedPassword and compare it to the real hash
    	
    	return true;
    }
    
    
    @Get
    public byte[] renderHomePage(HelloWorldController root) throws UnsupportedEncodingException {
        return "<html>\n<body><h1>hello world</h1></body></html>".getBytes("UTF-8");
        //return new ModelAndView("controller", this, "homePage");
    }

    @Root
    public HelloWorldController getRoot() {
        return this;
    }    
    

    @ChildrenOf
    public List<Product> getProducts(HelloWorldController root) {
        return products;
    }

    @ChildrenOf
    public List<ProductFile> getProductFiles(Product product) {
        return product.getProductFiles();
    }
     
     
    public class Product {
        private String name;
        private List<ProductFile> productFiles = new ArrayList<ProductFile>();
 
        public Product(String name) {
            this.name = name;
        }
 
        public String getName() {
            return name;
        }             
 
        public List<ProductFile> getProductFiles() {
            return productFiles;
        }                
    }

    public class ProductFile {
        private String name;
        private byte[] bytes;
 
        public ProductFile(String name, byte[] bytes) {
            this.name = name;
            this.bytes = bytes;
        }
 
        public String getName() {
            return name;
        }                
    }
}
