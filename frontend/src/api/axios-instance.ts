/**
 * Custom Axios instance for Orval-generated hooks
 *
 * This provides a configured axios client with:
 * - Base URL configuration
 * - Auth token interceptor
 * - Error handling (401 unauthorized)
 */

import axios, { type AxiosRequestConfig, type AxiosError } from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || '';

export const axiosInstance = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add auth token interceptor
axiosInstance.interceptors.request.use((config) => {
  const token = localStorage.getItem('auth_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Add error handling interceptor
axiosInstance.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    if (error.response?.status === 401) {
      // Handle unauthorized - clear token
      localStorage.removeItem('auth_token');
      // Could add redirect to login page here when auth is implemented
    }
    return Promise.reject(error);
  }
);

/**
 * Custom instance for Orval
 * Orval generates functions that call this with the endpoint config
 */
export const customInstance = <T>(config: AxiosRequestConfig): Promise<T> => {
  const source = axios.CancelToken.source();
  const promise = axiosInstance({
    ...config,
    cancelToken: source.token,
  }).then(({ data }) => data);

  // @ts-expect-error - Adding cancel method to promise for React Query
  promise.cancel = () => {
    source.cancel('Query was cancelled');
  };

  return promise;
};

export default axiosInstance;
